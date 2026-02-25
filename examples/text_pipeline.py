import csv
import logging
import string
from collections import Counter
from pathlib import Path

from dataprocessor import Pipeline
from dataprocessor import get_logger

######### I/O methods #########


def save_text(data: list[str], filename: Path | str) -> None:
    """Saves a list of strings to a text file, one string per line."""
    with open(filename, "w") as f:
        f.writelines(line + "\n" for line in data)


def load_text(filename: Path | str) -> list[str]:
    """Loads a list of strings from a text file, one string per line."""
    with open(filename) as f:
        return [line.strip() for line in f]


def save_tokens_csv(data: list[list[str]], filename: Path | str) -> None:
    """Saves a list of lists of strings to a CSV file, one list per line."""
    with open(filename, "w") as f:
        writer = csv.writer(f)
        writer.writerows(data)


def load_tokens_csv(filename: Path | str) -> list[list[str]]:
    """Loads a list of lists of strings from a CSV file, one list per line."""
    with open(filename) as f:
        reader = csv.reader(f)
        return [row for row in reader]


def save_word_freq(data: dict[str, int], filename: Path | str) -> None:
    """Saves a dictionary of word frequencies to a text file."""
    with open(filename, "w") as f:
        f.writelines(f"{word},{freq}\n" for word, freq in data.items())


####### Data processors #######


def lowercase(data: list[str]) -> list[str]:
    return [s.lower() for s in data]


def remove_punctuation(data: list[str]) -> list[str]:
    table = str.maketrans("", "", string.punctuation)
    return [s.translate(table) for s in data]


def tokenize(data: list[str]) -> list[list[str]]:
    return [s.split() for s in data]


def remove_stopwords(data: list[list[str]], stopwords: set[str]) -> list[list[str]]:
    return [[w for w in words if w not in stopwords] for words in data]


def word_frequency(data: list[list[str]], most_common: int = 5) -> dict[str, int]:
    all_words = [w for words in data for w in words]
    items: dict[str, int] = dict(Counter(all_words).most_common(most_common))
    return items


def main() -> None:

    data_path = Path("./examples/data/text/")

    logger = get_logger()
    logger.setLevel(logging.INFO)

    STOPWORDS = {
        "is",
        "are",
        "a",
        "for",
        "this",
        "the",
        "and",
        "again",
        "let",
        "lets",
        "some",
        "they",
        "to",
        "from",
        "with",
        "of",
        "in",
        "by",
        "that",
        "their",
        "than",
        "it",
        "as",
        "not",
    }

    pipeline = Pipeline(force_run=False, metadata_path=data_path / "pipeline_metadata.json")

    pipeline.add_step(
        name="lowercase",
        processor=lowercase,
        input_path=data_path / "input.txt",
        output_path=data_path / "lowercase.txt",
        save_method=save_text,
        load_method=load_text,
    )

    pipeline.add_step(
        name="remove_punctuation",
        processor=remove_punctuation,
        inputs="lowercase",
        output_path=data_path / "no_punctuation.txt",
        save_method=save_text,
        load_method=load_text,
    )

    pipeline.add_step(
        name="tokenize",
        processor=tokenize,
        inputs="remove_punctuation",
        output_path=data_path / "tokens.csv",
        save_method=save_tokens_csv,
        load_method=load_tokens_csv,
    )

    pipeline.add_step(
        name="remove_stopwords",
        processor=remove_stopwords,
        inputs="tokenize",
        params={"stopwords": STOPWORDS},
        output_path=data_path / "tokens_filtered.csv",
        save_method=save_tokens_csv,
        load_method=load_tokens_csv,
    )

    pipeline.add_step(
        name="word_frequency",
        processor=word_frequency,
        params={"most_common": 10},
        inputs="remove_stopwords",
        output_path=data_path / "word_frequency.txt",
        save_method=save_word_freq,
    )

    # Run pipeline
    pipeline.validate_step_types()
    pipeline.run()

    # Retrieve result
    result: dict[str, int] = pipeline.get_output("word_frequency")

    print("Word frequencies:")
    for word, freq in result.items():
        print(f"    {word}: {freq}")


if __name__ == "__main__":
    main()
