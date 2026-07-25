"""
accuracy_booster.py  (CLI wrapper — singular, matches Go wrapper call)

Go calls:  python accuracy_booster.py --model <path> --boost
This script is a thin CLI shim around the AccuracyBoosters library.

--boost:    runs temperature calibration + TTA sanity-check on a sample image
            and prints a JSON summary the Go wrapper can read.
--evaluate: (optional) run full eval on a data directory and report accuracy.
"""

import argparse
import json
import sys
import os
import traceback
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description="Accuracy booster CLI")
    p.add_argument("--model",    required=True,  help="Path to .h5 model")
    p.add_argument("--boost",    action="store_true", help="Verify TTA + calibration work")
    p.add_argument("--evaluate", action="store_true", help="Full eval on --data-dir")
    p.add_argument("--data-dir", default=None,   help="Data dir for --evaluate")
    p.add_argument("--temperature", type=float, default=1.5)
    p.add_argument("--tta-count",   type=int,   default=5)
    p.add_argument("--json",     action="store_true")
    return p.parse_args()


def _fail(msg, as_json):
    if as_json:
        print(json.dumps({"success": False, "error": msg}))
    else:
        print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def main():
    args = parse_args()

    model_path = Path(args.model)
    if not model_path.exists():
        _fail(f"Model not found: {model_path}", args.json)

    try:
        import tensorflow as tf
        import numpy as np
    except ImportError as e:
        _fail(f"TensorFlow not available: {e}", args.json)

    try:
        from accuracy_boosters import AccuracyBoosters, _safe_normalize
    except ImportError as e:
        _fail(f"Cannot import accuracy_boosters: {e}", args.json)

    # Load model
    try:
        model = tf.keras.models.load_model(str(model_path), compile=False)
    except Exception as e:
        _fail(f"Failed to load model: {e}", args.json)

    output_shape = model.output_shape
    if isinstance(output_shape, dict):
        num_classes = output_shape['class'][-1]
    else:
        num_classes = output_shape[-1]

    result = {
        "success":     True,
        "model":       str(model_path),
        "num_classes": int(num_classes),
    }

    if args.boost:
        # Smoke-test with a synthetic random image
        dummy_input = model.input_shape
        if isinstance(dummy_input, list):
            dummy_input = dummy_input[0]
        # shape is (None, H, W, 3) or (None, angles, H, W, 3)
        img_shape = tuple(d if d is not None else 1 for d in dummy_input[1:])
        dummy = np.random.rand(*img_shape).astype(np.float32)

        # If the model expects an angles dimension, use the first angle only for TTA
        if len(img_shape) == 4:
            # [angles, H, W, 3] — collapse angles for TTA
            tta_image = dummy[0]
        else:
            tta_image = dummy

        try:
            probs = AccuracyBoosters.add_test_time_augmentation(
                model, tta_image, num_augmentations=args.tta_count)
            calibrated = AccuracyBoosters.calibrate_confidence(
                probs, temperature=args.temperature)

            result.update({
                "tta_ok":          True,
                "calibration_ok":  True,
                "top_class":       int(np.argmax(calibrated)),
                "top_confidence":  float(np.max(calibrated)),
                "temperature":     args.temperature,
                "tta_count":       args.tta_count,
            })
        except Exception as e:
            result.update({
                "tta_ok":  False,
                "error":   str(e),
                "traceback": traceback.format_exc(),
            })

    if args.evaluate and args.data_dir:
        data_dir = Path(args.data_dir)
        if not data_dir.exists():
            result["evaluate_error"] = f"data-dir not found: {data_dir}"
        else:
            # Load metadata for class map
            meta_path = model_path.parent / f"{model_path.stem}_metadata.json"
            idx_to_pid = {}
            input_size = 224
            if meta_path.exists():
                with open(meta_path) as f:
                    meta = json.load(f)
                idx_to_pid = {int(k): v for k, v in meta.get("idx_to_class", {}).items()}
                input_size = meta.get("input_size", 224)

            correct, total = 0, 0
            for class_dir in sorted(data_dir.iterdir()):
                if not class_dir.is_dir():
                    continue
                pid = class_dir.name
                for img_file in list(class_dir.glob("*.jpg"))[:5]:  # sample 5 per class
                    try:
                        raw = tf.io.read_file(str(img_file))
                        img = tf.image.decode_image(raw, channels=3, expand_animations=False)
                        img = tf.image.resize(img, [input_size, input_size])
                        img = tf.cast(img, tf.float32).numpy() / 255.0

                        probs = AccuracyBoosters.add_test_time_augmentation(model, img)
                        pred_idx = int(np.argmax(probs))
                        pred_pid = idx_to_pid.get(pred_idx, str(pred_idx))

                        if pred_pid == pid:
                            correct += 1
                        total += 1
                    except Exception:
                        continue

            accuracy = correct / total if total > 0 else 0.0
            result.update({
                "evaluate_accuracy": accuracy,
                "evaluate_correct":  correct,
                "evaluate_total":    total,
            })

    if args.json:
        print(json.dumps(result))
    else:
        for k, v in result.items():
            print(f"{k}: {v}")


if __name__ == "__main__":
    main()