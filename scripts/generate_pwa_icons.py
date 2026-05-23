"""Generate PWA app icons: dark tile + Hebrew מ glyph in accent.

Sizes: 192, 512, 512 (maskable, with safe-zone padding), 180 (iOS).
Run from repo root: python3 scripts/generate_pwa_icons.py
"""
import os
from PIL import Image, ImageDraw, ImageFont

BG = (15, 23, 42, 255)       # #0f172a
ACCENT = (99, 102, 241, 255) # #6366f1

OUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'static', 'icons')
os.makedirs(OUT_DIR, exist_ok=True)

FONT_CANDIDATES = [
    '/System/Library/Fonts/ArialHB.ttc',
    '/System/Library/Fonts/Supplemental/Arial Hebrew.ttc',
    '/usr/share/fonts/truetype/noto/NotoSansHebrew-Bold.ttf',
    '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
    '/System/Library/Fonts/Helvetica.ttc',
]


def find_font(size):
    for p in FONT_CANDIDATES:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                continue
    return ImageFont.load_default()


def draw_tile(size, glyph_ratio=0.62, corner_ratio=0.22):
    """Rounded dark tile with centered Hebrew מ in accent."""
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    radius = int(size * corner_ratio)
    draw.rounded_rectangle([(0, 0), (size, size)], radius=radius, fill=BG)

    glyph_size = int(size * glyph_ratio)
    font = find_font(glyph_size)
    text = 'מ'
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    x = (size - tw) // 2 - bbox[0]
    y = (size - th) // 2 - bbox[1]
    draw.text((x, y), text, font=font, fill=ACCENT)
    return img


def save(img, name):
    path = os.path.join(OUT_DIR, name)
    img.save(path, 'PNG')
    print(f'wrote {path} ({img.size[0]}x{img.size[1]})')


def main():
    # Standard icons — glyph fills most of the tile
    save(draw_tile(192, glyph_ratio=0.62), 'icon-192.png')
    save(draw_tile(512, glyph_ratio=0.62), 'icon-512.png')
    save(draw_tile(180, glyph_ratio=0.62, corner_ratio=0.22), 'icon-180.png')

    # Maskable — Android may crop ~20% on each side. Keep glyph in safe zone (~40% radius).
    # Full-bleed background, smaller glyph centered.
    maskable = Image.new('RGBA', (512, 512), BG)
    draw = ImageDraw.Draw(maskable)
    glyph_size = int(512 * 0.42)
    font = find_font(glyph_size)
    text = 'מ'
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    x = (512 - tw) // 2 - bbox[0]
    y = (512 - th) // 2 - bbox[1]
    draw.text((x, y), text, font=font, fill=ACCENT)
    save(maskable, 'icon-512-maskable.png')


if __name__ == '__main__':
    main()
