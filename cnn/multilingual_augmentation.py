"""
multilingual_augmentation.py

Called by Go CNNWrapper.RunMultilingualAugmentation():
    python multilingual_augmentation.py --data-dir <dir> --apply

Augments product images with synthetic text overlays in English and Arabic
so the model sees both scripts during training even if some products only
have one language on their packaging.
"""

import argparse
import json
import sys
import os
import traceback
import random
from pathlib import Path


# ── Arabic / English sample strings ──────────────────────────────────────────
# Real augmentation would pull from product labels; these are stand-ins
# that exercise both Unicode directions and character shapes.
ENGLISH_STRINGS = [
    "Product", "Fresh", "Quality", "Natural", "Premium",
    "Organic", "500g", "1kg", "Best Before", "Net Weight",
]
ARABIC_STRINGS = [
    "منتج", "طازج", "جودة", "طبيعي", "ممتاز",
    "عضوي", "٥٠٠ غ", "١ كغ", "أفضل قبل", "الوزن الصافي",
]


def parse_args():
    p = argparse.ArgumentParser(description="Multilingual image augmentation")
    p.add_argument("--data-dir",   required=True)
    p.add_argument("--apply",      action="store_true",
                   help="Write augmented images alongside originals")
    p.add_argument("--dry-run",    action="store_true",
                   help="Count eligible images without writing")
    p.add_argument("--per-image",  type=int, default=2,
                   help="Augmented variants to generate per source image")
    p.add_argument("--json",       action="store_true")
    return p.parse_args()


def _fail(msg, as_json):
    if as_json:
        print(json.dumps({"success": False, "error": msg}))
    else:
        print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def augment_with_text(img_array, text: str, lang: str):
    """
    Overlay a semi-transparent text label on the image.
    Uses PIL — falls back to returning the original if PIL is unavailable.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        import numpy as np

        pil_img = Image.fromarray((img_array * 255).astype("uint8")).convert("RGBA")
        overlay = Image.new("RGBA", pil_img.size, (255, 255, 255, 0))
        draw    = ImageDraw.Draw(overlay)

        # Position: random quadrant
        w, h   = pil_img.size
        x = random.randint(0, w // 2)
        y = random.randint(0, h // 2)

        # Semi-transparent label band
        text_h = 24
        draw.rectangle([x, y, min(x + 200, w), y + text_h],
                       fill=(0, 0, 0, 100))

        # Try to find a font; fallback to default
        try:
            font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
        except Exception:
            font = ImageFont.load_default()

        draw.text((x + 4, y + 4), text, fill=(255, 255, 255, 220), font=font)
        combined = Image.alpha_composite(pil_img, overlay).convert("RGB")
        return np.array(combined).astype("float32") / 255.0

    except ImportError:
        # PIL not available — return original unchanged
        return img_array


def process_data_dir(data_dir: Path, apply: bool, per_image: int) -> dict:
    """Walk data_dir/pid/img.jpg and optionally write augmented variants."""
    try:
        import numpy as np
        import tensorflow as tf
    except ImportError as e:
        return {"success": False, "error": f"TensorFlow not available: {e}"}

    total_source  = 0
    total_written = 0
    skipped       = 0
    errors        = []

    for class_dir in sorted(data_dir.iterdir()):
        if not class_dir.is_dir():
            continue

        image_files = []
        for ext in (".jpg", ".jpeg", ".png"):
            image_files.extend(class_dir.glob(f"*{ext}"))
        image_files = sorted(image_files)

        for img_path in image_files:
            # Skip already-augmented images
            if "_aug_" in img_path.stem:
                skipped += 1
                continue

            total_source += 1

            if not apply:
                continue  # dry-run: just count

            try:
                raw  = tf.io.read_file(str(img_path))
                img  = tf.image.decode_image(raw, channels=3, expand_animations=False)
                img  = tf.image.resize(img, [224, 224])
                arr  = tf.cast(img, tf.float32).numpy() / 255.0
            except Exception as e:
                errors.append(f"{img_path}: {e}")
                continue

            for i in range(per_image):
                lang   = random.choice(["en", "ar"])
                text   = (random.choice(ENGLISH_STRINGS) if lang == "en"
                          else random.choice(ARABIC_STRINGS))
                augmented = augment_with_text(arr, text, lang)

                # Save as JPEG alongside original
                out_name = f"{img_path.stem}_aug_{lang}_{i:02d}.jpg"
                out_path = class_dir / out_name

                try:
                    from PIL import Image
                    import numpy as np
                    pil = Image.fromarray((augmented * 255).astype("uint8"))
                    pil.save(str(out_path), "JPEG", quality=90)
                    total_written += 1
                except Exception as e:
                    # PIL save failed — write via TF
                    try:
                        enc = tf.image.encode_jpeg(
                            tf.cast(augmented * 255, tf.uint8))
                        tf.io.write_file(str(out_path), enc)
                        total_written += 1
                    except Exception as e2:
                        errors.append(f"{out_path}: {e2}")

    return {
        "success":       True,
        "source_images": total_source,
        "written":       total_written,
        "skipped":       skipped,
        "errors":        errors[:20],   # cap error list
        "dry_run":       not apply,
    }


def main():
    args = parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        _fail(f"data-dir not found: {data_dir}", args.json)

    if not args.apply and not args.dry_run:
        _fail("Pass --apply to write augmented images or --dry-run to preview.", args.json)

    try:
        result = process_data_dir(data_dir, apply=args.apply, per_image=args.per_image)
    except Exception as e:
        _fail(f"Augmentation failed: {e}\n{traceback.format_exc()}", args.json)

    if args.json:
        print(json.dumps(result))
    else:
        for k, v in result.items():
            print(f"{k}: {v}")


if __name__ == "__main__":
    main()