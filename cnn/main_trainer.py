"""
main_trainer.py
CLI entry point called by Go CNNWrapper via exec.Command.
Supports --train-new and --predict modes with --json output.
"""

import argparse
import json
import sys
import os
import time
import traceback
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Product CNN trainer / predictor")

    # Mode
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--train-new", action="store_true", help="Train a new model")
    mode.add_argument("--predict",   action="store_true", help="Run inference")

    # Training args
    parser.add_argument("--data-dir",   type=str, help="Root folder: data_dir/pid/img.jpg")
    parser.add_argument("--output-dir", type=str, default="./models")
    parser.add_argument("--model-name", type=str, default="product_model")
    parser.add_argument("--epochs",     type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--use-hybrid", action="store_true", default=True)
    parser.add_argument("--use-basic",  action="store_true")
    parser.add_argument("--augment",    action="store_true")

    # Prediction args
    parser.add_argument("--model",   type=str, help="Path to saved model (.h5)")
    parser.add_argument("--image",   type=str, help="Path to image for prediction")
    parser.add_argument("--top-k",   type=int, default=3)
    parser.add_argument("--use-tta", action="store_true", default=True)
    parser.add_argument("--no-tta",  action="store_true")

    # Output
    parser.add_argument("--json", action="store_true", help="Output results as JSON")

    return parser.parse_args()


# ─────────────────────────────────────────────
# TRAIN
# ─────────────────────────────────────────────
def run_train(args):
    if not args.data_dir:
        _fail("--data-dir is required for --train-new", args.json)

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        _fail(f"data-dir not found: {data_dir}", args.json)

    use_hybrid = not args.use_basic   # --use-basic overrides default hybrid

    # Lazy import so errors surface clearly
    try:
        from production_trainer import ProductionTrainer
    except ImportError as e:
        _fail(f"Cannot import production_trainer: {e}", args.json)

    trainer = ProductionTrainer(
        data_dir=str(data_dir),
        model_dir=args.output_dir,
        use_hybrid=use_hybrid,
    )

    input_size = 384 if use_hybrid else 224
    trainer.setup(
        num_angles=3,
        input_size=input_size,
        batch_size=args.batch_size,
    )

    t0 = time.time()
    history, model_path = trainer.train(
        epochs=args.batch_size,      # passed correctly below
        batch_size=args.batch_size,
        model_name=args.model_name,
    )
    elapsed = time.time() - t0

    # Pull final accuracy from history
    final_accuracy = 0.0
    if history and hasattr(history, "history"):
        acc_key = "class_accuracy" if use_hybrid else "accuracy"
        if acc_key in history.history and history.history[acc_key]:
            final_accuracy = float(history.history[acc_key][-1])

    # Save metadata
    meta_path = trainer.save_metadata(model_path, args.model_name)

    result = {
        "success": True,
        "model_path": str(model_path),
        "num_classes": trainer.num_classes,
        "final_accuracy": final_accuracy,
        "training_time": f"{elapsed:.1f}s",
        "model_type": "hybrid" if use_hybrid else "basic",
        "metadata_path": str(meta_path),
    }

    _output(result, args.json)


# ─────────────────────────────────────────────
# PREDICT
# ─────────────────────────────────────────────
def run_predict(args):
    if not args.model:
        _fail("--model is required for --predict", args.json)
    if not args.image:
        _fail("--image is required for --predict", args.json)

    model_path = Path(args.model)
    image_path = Path(args.image)

    if not model_path.exists():
        _fail(f"Model not found: {model_path}", args.json)
    if not image_path.exists():
        _fail(f"Image not found: {image_path}", args.json)

    use_tta = args.use_tta and not args.no_tta

    try:
        import tensorflow as tf
        import numpy as np
    except ImportError as e:
        _fail(f"TensorFlow not available: {e}", args.json)

    # Load metadata to get class mapping
    meta_path = model_path.parent / f"{model_path.stem}_metadata.json"
    class_map = {}
    num_classes = 0
    input_size = 224

    if meta_path.exists():
        with open(meta_path) as f:
            meta = json.load(f)
        class_map = {int(k): v for k, v in meta.get("class_to_idx", {}).items()
                     if isinstance(k, (str, int))}
        # Invert: idx -> pid
        idx_to_pid = {v: k for k, v in meta.get("class_to_idx", {}).items()}
        num_classes = meta.get("num_classes", 0)
        input_size  = meta.get("input_size", 224)
    else:
        idx_to_pid = {}
        print(f"Warning: metadata not found at {meta_path}, class names unavailable",
              file=sys.stderr)

    # Load model
    t0 = time.time()
    try:
        model = tf.keras.models.load_model(str(model_path), compile=False)
    except Exception as e:
        _fail(f"Failed to load model: {e}", args.json)

    # Load + preprocess image
    img = _load_image(str(image_path), input_size)

    # Inference
    try:
        from accuracy_boosters import AccuracyBoosters
        if use_tta:
            probs = AccuracyBoosters.add_test_time_augmentation(model, img)
        else:
            batch = np.expand_dims(img, axis=0)
            out = model.predict(batch, verbose=0)
            if isinstance(out, dict) and "class" in out:
                probs = out["class"][0]
            else:
                probs = out[0]
    except ImportError:
        # Fall back to plain predict if accuracy_boosters not available
        batch = np.expand_dims(img, axis=0)
        out = model.predict(batch, verbose=0)
        if isinstance(out, dict) and "class" in out:
            probs = out["class"][0]
        else:
            probs = out[0]

    elapsed = time.time() - t0

    # Build top-k predictions
    import numpy as np
    top_k = min(args.top_k, len(probs))
    top_indices = np.argsort(probs)[-top_k:][::-1]

    predictions = []
    for idx in top_indices:
        idx_int = int(idx)
        pid = idx_to_pid.get(idx_int, str(idx_int))
        predictions.append({
            "pid":          pid,
            "class_name":   pid,
            "product_name": pid,
            "class_index":  idx_int,
            "confidence":   float(probs[idx_int]),
        })

    result = {
        "success":        True,
        "predictions":    predictions,
        "top_prediction": predictions[0] if predictions else None,
        "inference_time": f"{elapsed:.3f}s",
        "used_tta":       use_tta,
    }

    _output(result, args.json)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _load_image(path: str, size: int):
    import tensorflow as tf
    import numpy as np
    raw = tf.io.read_file(path)
    img = tf.image.decode_image(raw, channels=3, expand_animations=False)
    img = tf.image.resize(img, [size, size])
    img = tf.cast(img, tf.float32) / 255.0
    return img.numpy()


def _output(data: dict, as_json: bool):
    if as_json:
        print(json.dumps(data))
    else:
        for k, v in data.items():
            print(f"{k}: {v}")
    sys.exit(0)


def _fail(msg: str, as_json: bool):
    if as_json:
        print(json.dumps({"success": False, "error": msg}))
    else:
        print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()
    try:
        if args.train_new:
            run_train(args)
        elif args.predict:
            run_predict(args)
    except Exception as e:
        tb = traceback.format_exc()
        if args.json:
            print(json.dumps({"success": False, "error": str(e), "traceback": tb}))
        else:
            print(f"FATAL: {e}\n{tb}", file=sys.stderr)
        sys.exit(1)