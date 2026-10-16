from io import StringIO
from itertools import combinations
from typing import Any

import yaml
from Bio import Align
from Bio import SeqIO
from Bio.SeqRecord import SeqRecord

from dataprocessor import Pipeline

DNA_ALPHABET = set("ACGTN")
MOTIF = "GAATTC"

# Three short, synthetic sequences:
# - sample_alpha has one substitution relative to the reference.
# - sample_beta contains an additional occurrence of the motif.
FASTA_DATA = """>reference
ACGTACGTGAATTCACGTACGT
>sample_alpha
ACGTTCGTGAATTCACGTACGT
>sample_beta
ACGTACGTGAATTCACGTGAATTC
"""


def parse_fasta(fasta_text: str) -> list[SeqRecord]:
    """Parse FASTA text and validate that records contain DNA sequences."""
    records = list(SeqIO.parse(StringIO(fasta_text), "fasta"))

    if not records:
        raise ValueError("No sequences found in FASTA input.")

    identifiers = [record.id for record in records]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("Sequence identifiers must be unique.")

    for record in records:
        sequence = str(record.seq).upper()

        if not sequence:
            raise ValueError(f"Sequence {record.id!r} is empty.")

        invalid_bases = set(sequence) - DNA_ALPHABET
        if invalid_bases:
            raise ValueError(f"Sequence {record.id!r} contains invalid DNA symbols: {sorted(invalid_bases)}")

        record.seq = record.seq.upper()

    return records


def calculate_sequence_qc(
    records: list[SeqRecord],
) -> list[dict[str, Any]]:
    """Calculate sequence length and GC content for each record."""
    results = []

    for record in records:
        sequence = str(record.seq)
        unambiguous_bases = [base for base in sequence if base in {"A", "C", "G", "T"}]

        gc_content = (
            100.0 * sum(base in {"G", "C"} for base in unambiguous_bases) / len(unambiguous_bases)
            if unambiguous_bases
            else 0.0
        )

        results.append(
            {
                "id": record.id,
                "length": len(sequence),
                "gc_content_percent": round(gc_content, 2),
            }
        )

    return results


def scan_motifs(
    records: list[SeqRecord],
    motif: str,
) -> tuple[dict[str, list[int]], int]:
    """
    Find all motif occurrences, including overlapping occurrences.

    Positions are zero-based offsets in the original sequence.
    """
    motif = motif.upper()

    if not motif or not set(motif) <= DNA_ALPHABET:
        raise ValueError("Motif must contain valid, non-empty DNA symbols.")

    positions_by_sequence = {}

    for record in records:
        sequence = str(record.seq)
        positions = []
        start = 0

        while True:
            position = sequence.find(motif, start)
            if position == -1:
                break

            positions.append(position)
            start = position + 1

        positions_by_sequence[record.id] = positions

    total_hits = sum(len(positions) for positions in positions_by_sequence.values())

    return positions_by_sequence, total_hits


def compare_sequences(
    records: list[SeqRecord],
) -> list[dict[str, Any]]:
    """Globally align each pair of sequences and summarize the alignments."""
    aligner = Align.PairwiseAligner()
    aligner.mode = "global"
    aligner.match_score = 2.0
    aligner.mismatch_score = -1.0
    aligner.open_gap_score = -2.0
    aligner.extend_gap_score = -0.5

    results = []

    for first, second in combinations(records, 2):
        alignment = aligner.align(first.seq, second.seq)[0]
        counts = alignment.counts()

        results.append(
            {
                "sequence_a": first.id,
                "sequence_b": second.id,
                "alignment_score": float(alignment.score),
                "identities": int(counts.identities),
                "mismatches": int(counts.mismatches),
                "gaps": int(counts.gaps),
            }
        )

    return results


def build_report(
    qc_results: list[dict[str, Any]],
    motif_positions: dict[str, list[int]],
    alignments: list[dict[str, Any]],
    total_motif_hits: int,
) -> dict[str, Any]:
    """Combine the outputs of independent analysis steps."""
    return {
        "sequence_count": len(qc_results),
        "sequence_qc": qc_results,
        "motif": MOTIF,
        "motif_positions_zero_based": motif_positions,
        "total_motif_hits": total_motif_hits,
        "pairwise_alignments": alignments,
    }


def main() -> None:
    print(f"""
Sequence analysis pipeline:
    - Parse sequence records from FASTA format.
    - Calculate sequence length and GC ratio.
    - Locate motif {MOTIF} in sequence.
    - Align all prossible sequence pairs.
    - Compile statistics from analysis.
""")

    pipeline = Pipeline()

    pipeline.add_step(
        name="parse_fasta",
        processor=parse_fasta,
        input_data=FASTA_DATA,
    )

    pipeline.add_step(
        name="sequence_qc",
        processor=calculate_sequence_qc,
        inputs="parse_fasta",
    )

    pipeline.add_step(
        name="scan_motifs",
        processor=scan_motifs,
        inputs="parse_fasta",
        params={"motif": MOTIF},
        outputs=["motif_positions", "total_motif_hits"],
    )

    pipeline.add_step(
        name="pairwise_alignment",
        processor=compare_sequences,
        inputs="parse_fasta",
    )

    pipeline.add_step(
        name="report",
        processor=build_report,
        inputs=[
            "sequence_qc",
            "scan_motifs.motif_positions",
            "pairwise_alignment",
            "scan_motifs.total_motif_hits",
        ],
    )

    pipeline.validate_step_types()
    pipeline.run(parallel="thread", max_workers=3)

    qc_results = pipeline.get_output("sequence_qc")
    motif_positions = pipeline.get_output("scan_motifs.motif_positions")
    total_motif_hits = pipeline.get_output("scan_motifs.total_motif_hits")
    alignments = pipeline.get_output("pairwise_alignment")
    report = pipeline.get_output("report")

    print("\nResults: ***********************************\n")
    print(yaml.dump(report, sort_keys=False))
    print("********************************************")

    assert len(qc_results) == 3
    assert {result["id"] for result in qc_results} == {
        "reference",
        "sample_alpha",
        "sample_beta",
    }

    assert motif_positions == {
        "reference": [8],
        "sample_alpha": [8],
        "sample_beta": [8, 18],
    }
    assert total_motif_hits == 4

    assert len(alignments) == 3

    reference_comparison = next(
        result
        for result in alignments
        if result["sequence_a"] == "reference" and result["sequence_b"] == "sample_alpha"
    )
    assert reference_comparison["mismatches"] == 1
    assert reference_comparison["gaps"] == 0

    assert report["sequence_count"] == 3
    assert report["total_motif_hits"] == 4


if __name__ == "__main__":
    main()
