import logging
import string
from collections import Counter

from dataprocessor import Pipeline
from dataprocessor import get_logger

######### I/O methods #########


def save_text(data: list[str], filename: str) -> None:
    """Saves a list of strings to a text file, one string per line."""
    with open(filename, "w") as f:
        f.writelines(line + "\n" for line in data)


def load_text(filename: str) -> list[str]:
    """Loads a list of strings from a text file, one string per line."""
    with open(filename) as f:
        return [line.strip() for line in f]


def save_word_freq(data: dict[str, int], filename: str) -> None:
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


def word_frequency(data: list[list[str]]) -> dict[str, int]:
    all_words = [w for words in data for w in words]
    items: dict[str, int] = dict(Counter(all_words))
    return items


def main() -> None:

    logger = get_logger()
    logger.setLevel(logging.DEBUG)

    STOPWORDS = {"is", "a", "for", "this", "the", "and", "again", "let", "lets", "some"}

    input_data = [
        "Hello, world! This is a test.",
        "Python is great for text processing.",
        "Hello again; let's process some text!",
    ]

    # Set up pipeline
    pipeline = Pipeline()

    # Add steps
    pipeline.add_step(
        name="lowercase",
        processor=lowercase,
        input_data=input_data,
        output_path="./examples/data/text/lowercase.txt",
        save_method=save_text,
        load_method=load_text,
    )
    pipeline.add_step(
        name="remove_punctuation",
        processor=remove_punctuation,
        inputs="lowercase",
        output_path="./examples/data/text/no_punctuation.txt",
        save_method=save_text,
        load_method=load_text,
    )
    pipeline.add_step(
        name="tokenize",
        processor=tokenize,
        inputs="remove_punctuation",
    )
    pipeline.add_step(
        name="remove_stopwords",
        processor=remove_stopwords,
        inputs="tokenize",
        params={"stopwords": STOPWORDS},
    )
    pipeline.add_step(
        name="word_frequency",
        processor=word_frequency,
        inputs="remove_stopwords",
        output_path="./examples/data/text/word_frequency.txt",
        save_method=save_word_freq,
    )

    # Run pipeline
    pipeline.validate_step_types()
    pipeline.run()

    # Retrieve result
    result: Counter = pipeline.get_output("word_frequency")

    print("Word frequencies:")
    for word, freq in result.items():
        print(f"    {word}: {freq}")


if __name__ == "__main__":
    main()
