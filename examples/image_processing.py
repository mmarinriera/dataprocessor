import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from numpy.typing import NDArray
from skimage import filters
from skimage import measure
from skimage import morphology

from dataprocessor import Pipeline
from dataprocessor import get_logger

Image = NDArray[np.float64]
LabelImage = NDArray[np.int32]

# Input data simulation


def generate_image(seed: int = 42) -> Image:
    """Generate a synthetic fluorescence microscopy image."""
    rng = np.random.default_rng(seed)

    height, width = 160, 160
    y, x = np.mgrid[:height, :width]

    # Place well-separated fluorescent objects.
    centers = [
        (25, 25),
        (25, 65),
        (25, 105),
        (25, 140),
        (65, 35),
        (65, 80),
        (65, 125),
        (105, 25),
        (105, 65),
        (105, 105),
        (105, 140),
        (140, 55),
    ]

    image = np.zeros((height, width), dtype=np.float64)

    for cy, cx in centers:
        amplitude = rng.uniform(0.8, 1.2)
        sigma = rng.uniform(2.5, 3.5)

        image += amplitude * np.exp(-((x - cx) ** 2 + (y - cy) ** 2) / (2 * sigma**2))

    image += rng.normal(0.08, 0.06, size=image.shape)

    return np.clip(image, 0.0, None)


# Processor functions


def preprocess(image: Image) -> Image:
    """Reduce high-frequency noise using Gaussian smoothing."""
    return filters.gaussian(
        image,
        sigma=1.0,
        preserve_range=True,
    )


def measure_image_quality(image: Image) -> tuple[float, float]:
    """Calculate mean intensity and intensity standard deviation."""
    return float(image.mean()), float(image.std())


def segment_objects(image: Image) -> LabelImage:
    """Segment fluorescent objects using global thresholding."""
    threshold = filters.threshold_otsu(image)
    foreground = image > threshold

    foreground = morphology.remove_small_objects(
        foreground,
        max_size=20,
    )

    return measure.label(foreground).astype(np.int32)


def count_objects(labels: LabelImage) -> int:
    """Count connected components in a labelled image."""
    return int(labels.max())


def mean_object_area(labels: LabelImage) -> float:
    """Calculate the mean area of segmented objects in pixels."""
    properties = measure.regionprops(labels)

    if not properties:
        return 0.0

    return float(np.mean([region.area for region in properties]))


def main() -> None:
    data_path = Path("./examples/data/image_processing/")

    logger = get_logger()
    logger.setLevel(logging.INFO)

    print("""
Image processing pipeline:
    - Simulate grayscale fluorescence image.
    - Remove background noise with Gaussian smoothing.
    - Segment foreground objects using Otsu Thresholding.
    - Analyse segmented objects.
    - Generate figure with intermediate and final results.
""")

    pipeline = Pipeline()

    pipeline.add_step(
        name="raw_image",
        processor=generate_image,
        input_data=42,
    )

    pipeline.add_step(
        name="preprocessed",
        processor=preprocess,
        inputs="raw_image",
    )

    pipeline.add_step(
        name="image_quality",
        processor=measure_image_quality,
        inputs="raw_image",
        outputs=["mean_intensity", "intensity_std"],
    )

    pipeline.add_step(
        name="segmentation",
        processor=segment_objects,
        inputs="preprocessed",
    )

    pipeline.add_step(
        name="object_count",
        processor=count_objects,
        inputs="segmentation",
    )

    pipeline.add_step(
        name="mean_area",
        processor=mean_object_area,
        inputs="segmentation",
    )

    pipeline.validate_step_types()
    pipeline.run(parallel="process", max_workers=2)

    raw = pipeline.get_output("raw_image")
    smoothed = pipeline.get_output("preprocessed")
    labels = pipeline.get_output("segmentation")

    count = pipeline.get_output("object_count")
    area = pipeline.get_output("mean_area")
    mean_intensity, intensity_std = pipeline.get_output("image_quality")

    print(f"""
Results:
    Image dimensions: {raw.shape}
    Detected objects: {count}
    Mean object area: {area:.1f} pixels
    Mean image intensity: {mean_intensity:.3f}
    Image intensity standard deviation: {intensity_std:.3f}
    """)

    assert raw.shape == (160, 160)
    assert smoothed.shape == raw.shape
    assert labels.shape == raw.shape
    assert 5 <= count <= 20
    assert area > 0
    assert np.isfinite(mean_intensity)
    assert np.isfinite(intensity_std)

    # Output figure
    figure, axes = plt.subplots(1, 3, figsize=(12, 5))

    axes[0].imshow(raw, cmap="gray")
    axes[0].set_title("Input image")

    axes[1].imshow(smoothed, cmap="gray")
    axes[1].set_title("Preprocessed image")

    axes[2].imshow(labels, cmap="nipy_spectral")
    axes[2].set_title(f"Segmentation ({count} objects)")

    for axis in axes:
        axis.axis("off")

    figure.tight_layout()
    figure.savefig(data_path / "image_outputs.png", dpi=150)
    plt.close(figure)


if __name__ == "__main__":
    main()
