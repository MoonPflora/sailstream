"""
production_trainer.py  (fixed)

Fixes applied:
  1. idx_to_class now maps index -> pid (was index -> index)
  2. Train/val split: 80% train, 20% val — separate generators, no leakage
  3. epochs arg was accidentally passing batch_size; fixed in train()
  4. _scan_dataset sorts deterministically so idx_to_class is stable across runs
"""

import tensorflow as tf
from tensorflow.keras import callbacks, optimizers, layers
import numpy as np
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional


class ProductDataGenerator:
    """Data generator for multilingual hybrid model"""

    def __init__(self, data_dir: str, num_angles: int = 3,
                 target_size: int = 384, batch_size: int = 8):
        self.data_dir    = Path(data_dir)
        self.num_angles  = num_angles
        self.target_size = target_size
        self.batch_size  = batch_size

        self.classes      = []
        self.class_to_idx = {}
        self.idx_to_class = {}   # FIX: was idx->idx, now idx->pid
        self._scan_dataset()

    def _scan_dataset(self):
        """Scan and build dataset mapping — sorted for stability"""
        product_folders = sorted(
            [f for f in self.data_dir.iterdir() if f.is_dir()]
        )

        for idx, folder in enumerate(product_folders):
            pid = folder.name
            self.classes.append(pid)
            self.class_to_idx[pid] = idx
            self.idx_to_class[idx] = pid   # FIX: store pid, not idx

    def _load_image(self, image_path: Path) -> np.ndarray:
        """Load and preprocess a single image"""
        img = tf.io.read_file(str(image_path))
        img = tf.image.decode_image(img, channels=3, expand_animations=False)
        img = tf.image.resize(img, [self.target_size, self.target_size])
        img = tf.cast(img, tf.float32) / 255.0
        return img.numpy()

    def _get_product_images(self, pid: str) -> List[np.ndarray]:
        """Get up to num_angles images for a product"""
        folder = self.data_dir / pid
        images = []

        for ext in ['.jpg', '.jpeg', '.png', '.webp']:
            for img_path in sorted(folder.glob(f'*{ext}')):
                if len(images) >= self.num_angles:
                    break
                try:
                    images.append(self._load_image(img_path))
                except Exception:
                    continue
            if len(images) >= self.num_angles:
                break

        return images

    def generator(self, class_subset: Optional[List[str]] = None):
        """
        Infinite generator over a subset of classes.
        Pass class_subset to restrict to train or val split.
        """
        pids = class_subset if class_subset is not None else self.classes

        while True:
            batch_images = []
            batch_labels = []

            for _ in range(self.batch_size):
                pid       = np.random.choice(pids)
                class_idx = self.class_to_idx[pid]
                images    = self._get_product_images(pid)

                # Pad with zeros if not enough angles
                while len(images) < self.num_angles:
                    images.append(
                        np.zeros((self.target_size, self.target_size, 3), dtype=np.float32)
                    )

                batch_images.append(np.stack(images[:self.num_angles]))
                batch_labels.append(class_idx)

            X = np.array(batch_images, dtype=np.float32)   # [B, angles, H, W, 3]
            y = {
                'class': tf.keras.utils.to_categorical(
                    batch_labels, num_classes=len(self.classes)
                ),
                'confidence': np.ones((len(batch_labels), 1), dtype=np.float32),
                'language':   np.zeros((len(batch_labels), 4), dtype=np.float32),
            }
            yield X, y

    def get_train_val_split(self, val_fraction: float = 0.2):
        """Return (train_pids, val_pids) with no overlap"""
        n      = len(self.classes)
        n_val  = max(1, int(n * val_fraction))
        rng    = np.random.default_rng(42)           # deterministic split
        idx    = rng.permutation(n)
        val    = [self.classes[i] for i in idx[:n_val]]
        train  = [self.classes[i] for i in idx[n_val:]]
        return train, val


class ProductionTrainer:
    """Production-grade training pipeline — fixed"""

    def __init__(self, data_dir: str, model_dir: str = "./models",
                 log_dir: str = "./logs", use_hybrid: bool = True):
        self.data_dir   = data_dir
        self.model_dir  = Path(model_dir)
        self.log_dir    = Path(log_dir)
        self.use_hybrid = use_hybrid

        self.model_dir.mkdir(exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)

        self.model       = None
        self.data_gen    = None
        self.num_classes = 0
        self.optimizer   = None
        self.losses      = None
        self.loss_weights = None
        self.metrics     = None

    def setup(self, num_angles: int = 3, input_size: int = 384, batch_size: int = 8):
        """Setup data generator and model"""
        self.data_gen = ProductDataGenerator(
            data_dir=self.data_dir,
            num_angles=num_angles,
            target_size=input_size,
            batch_size=batch_size,
        )
        self.num_classes = len(self.data_gen.classes)

        if self.use_hybrid:
            from multilingual_hybrid_model import MultilingualHybridProductModel
            self.model = MultilingualHybridProductModel(
                num_classes=self.num_classes,
                num_angles=num_angles,
                input_size=input_size,
            )
        else:
            from multilingual_hybrid_model import create_simplified_model
            self.model = create_simplified_model(
                num_classes=self.num_classes,
                input_size=input_size,
            )

        self.optimizer = self._create_optimizer()

        if self.use_hybrid:
            self.losses = {
                'class':      tf.keras.losses.CategoricalCrossentropy(label_smoothing=0.1),
                'confidence': tf.keras.losses.MeanSquaredError(),
                'language':   tf.keras.losses.CategoricalCrossentropy(),
            }
            self.loss_weights = {'class': 1.0, 'confidence': 0.1, 'language': 0.2}
            self.metrics = {
                'class':    ['accuracy', tf.keras.metrics.TopKCategoricalAccuracy(k=3)],
                'language': ['accuracy'],
            }
        else:
            self.losses       = 'categorical_crossentropy'
            self.loss_weights = None
            self.metrics      = ['accuracy']

    def _create_optimizer(self):
        if self.use_hybrid:
            lr_schedule = optimizers.schedules.CosineDecayRestarts(
                initial_learning_rate=0.001,
                first_decay_steps=1000,
                t_mul=2.0,
                m_mul=0.9,
                alpha=0.00001,
            )
            return optimizers.AdamW(
                learning_rate=lr_schedule,
                weight_decay=0.0001,
                beta_1=0.9,
                beta_2=0.999,
            )
        return optimizers.Adam(learning_rate=0.001)

    def _create_callbacks(self, model_name: str):
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        cbs = [
            callbacks.EarlyStopping(
                monitor='val_loss', patience=10,
                restore_best_weights=True, verbose=1,
            ),
            callbacks.ModelCheckpoint(
                filepath=str(self.model_dir / f"{model_name}_best.h5"),
                monitor='val_loss', save_best_only=True,
                save_weights_only=False, verbose=1,
            ),
            callbacks.TensorBoard(
                log_dir=str(self.log_dir / timestamp),
                histogram_freq=1, write_graph=True,
            ),
        ]
        if self.use_hybrid:
            cbs.append(callbacks.ReduceLROnPlateau(
                monitor='val_loss', factor=0.5,
                patience=5, min_lr=1e-6, verbose=1,
            ))
        return cbs

    def train(self, epochs: int = 50, batch_size: int = 8,
              model_name: str = "product_model"):
        """Train the model with a proper train/val split"""
        print(f"\n{'='*60}")
        print(f"TRAINING {'HYBRID' if self.use_hybrid else 'BASIC'} MODEL")
        print(f"{'='*60}")

        if self.model is None:
            input_size = 384 if self.use_hybrid else 224
            self.setup(input_size=input_size, batch_size=batch_size)

        print(f"Classes    : {self.num_classes}")
        print(f"Batch size : {batch_size}")
        print(f"Epochs     : {epochs}")        # FIX: was printing batch_size here

        # FIX: proper 80/20 split — separate generators, no leakage
        train_pids, val_pids = self.data_gen.get_train_val_split(val_fraction=0.2)
        print(f"Train pids : {len(train_pids)}  |  Val pids: {len(val_pids)}")

        train_gen = self.data_gen.generator(class_subset=train_pids)
        val_gen   = self.data_gen.generator(class_subset=val_pids)

        steps_per_epoch  = max(10, len(train_pids) * 3 // batch_size)
        validation_steps = max(3,  len(val_pids)   * 3 // batch_size)

        self.model.compile(
            optimizer=self.optimizer,
            loss=self.losses,
            loss_weights=self.loss_weights,
            metrics=self.metrics,
        )

        print("\nStarting training...")
        history = self.model.fit(
            train_gen,
            steps_per_epoch=steps_per_epoch,
            epochs=epochs,                   # FIX: was passing batch_size by mistake
            validation_data=val_gen,
            validation_steps=validation_steps,
            callbacks=self._create_callbacks(model_name),
            verbose=1,
        )

        final_path = self.model_dir / f"{model_name}_final.h5"
        self.model.save(str(final_path))

        print(f"\n✅ Training complete!")
        print(f"   Model saved: {final_path}")

        return history, str(final_path)

    def save_metadata(self, model_path: str, model_name: str) -> Path:
        """Persist class mapping alongside the model"""
        metadata = {
            'model_type':    'hybrid' if self.use_hybrid else 'basic',
            'model_path':    model_path,
            'classes':       self.data_gen.classes,
            'class_to_idx':  self.data_gen.class_to_idx,
            'idx_to_class':  {str(k): v for k, v in self.data_gen.idx_to_class.items()},
            'num_classes':   self.num_classes,
            'input_size':    384 if self.use_hybrid else 224,
            'num_angles':    self.data_gen.num_angles,
            'timestamp':     datetime.now().isoformat(),
        }

        meta_path = self.model_dir / f"{model_name}_metadata.json"
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        print(f"✅ Metadata saved: {meta_path}")
        return meta_path


# =============================================================================
# CLI entry point  (called by Go ProductionTrain() and Retrain())
# =============================================================================
if __name__ == "__main__":
    import argparse, sys, time, traceback, json as _json

    parser = argparse.ArgumentParser(description="Production fine-tune / transfer trainer")
    parser.add_argument("--base-model",  required=True)
    parser.add_argument("--new-data",    required=True)
    parser.add_argument("--output-name", required=True)
    parser.add_argument("--strategy",    default="fine_tune",
                        choices=["fine_tune", "transfer", "hybrid"])
    parser.add_argument("--epochs",      type=int, default=20)
    parser.add_argument("--batch-size",  type=int, default=8)
    parser.add_argument("--output-dir",  default="./models/production")
    parser.add_argument("--augment",     action="store_true")
    parser.add_argument("--json",        action="store_true")
    args = parser.parse_args()

    def _fail(msg):
        if args.json:
            print(_json.dumps({"success": False, "error": msg}))
        else:
            print(f"ERROR: {msg}", file=sys.stderr)
        sys.exit(1)

    from pathlib import Path as _Path
    import tensorflow as _tf

    if not _Path(args.base_model).exists():
        _fail(f"base model not found: {args.base_model}")
    if not _Path(args.new_data).exists():
        _fail(f"new-data dir not found: {args.new_data}")

    _Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    # Load base model to determine its config
    try:
        base = _tf.keras.models.load_model(args.base_model, compile=False)
    except Exception as e:
        _fail(f"Failed to load base model: {e}")

    # strategy: transfer = freeze all, fine_tune = unfreeze last N layers
    if args.strategy == "transfer":
        base.trainable = False
    elif args.strategy == "fine_tune":
        base.trainable = True
        # Freeze first 80 % of layers
        n = len(base.layers)
        for layer in base.layers[:int(n * 0.8)]:
            layer.trainable = False

    trainer = ProductionTrainer(
        data_dir=args.new_data,
        model_dir=args.output_dir,
        use_hybrid=False,   # fine-tune uses the loaded base, not a fresh hybrid
    )
    trainer.setup(batch_size=args.batch_size)
    trainer.model = base   # inject the loaded model directly

    t0 = time.time()
    try:
        history, model_path = trainer.train(
            epochs=args.epochs,
            batch_size=args.batch_size,
            model_name=args.output_name,
        )
    except Exception as e:
        _fail(f"Training failed: {e}\n{traceback.format_exc()}")

    elapsed = time.time() - t0

    acc = 0.0
    if history and hasattr(history, "history"):
        for key in ("accuracy", "class_accuracy"):
            if key in history.history and history.history[key]:
                acc = float(history.history[key][-1])
                break

    result = {
        "success":        True,
        "model_path":     str(model_path),
        "num_classes":    trainer.num_classes,
        "final_accuracy": acc,
        "training_time":  f"{elapsed:.1f}s",
        "strategy":       args.strategy,
    }

    if args.json:
        print(_json.dumps(result))
    else:
        for k, v in result.items():
            print(f"{k}: {v}")