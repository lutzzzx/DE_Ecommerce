"""
test_batch_loader.py
Unit test untuk fungsi validasi & sanity check di batch_loader.py,
menggunakan mock/tempfile sehingga tidak butuh MinIO nyata.
"""
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.append("ingestion")
import batch_loader  # noqa: E402


def test_validate_raw_files_exits_when_missing(monkeypatch, tmp_path):
    """Pastikan batch_loader berhenti (SystemExit) jika ada file CSV yang hilang."""
    monkeypatch.setattr(batch_loader, "RAW_DIR", tmp_path)
    with pytest.raises(SystemExit):
        batch_loader.validate_raw_files()


def test_validate_raw_files_passes_when_present(monkeypatch, tmp_path):
    """Pastikan batch_loader berhasil membaca ketika semua 9 file CSV tersedia."""
    monkeypatch.setattr(batch_loader, "RAW_DIR", tmp_path)
    for f in batch_loader.EXPECTED_FILES:
        (tmp_path / f).write_text("col1,col2\nval1,val2\n")
    files = batch_loader.validate_raw_files()
    assert len(files) == len(batch_loader.EXPECTED_FILES)


def test_quick_row_count_check_fails_on_empty_df(monkeypatch, tmp_path, capsys):
    """Pastikan error diangkat jika ada file CSV yang kosong (hanya header saja)."""
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("col1,col2\n")  # header saja, tanpa baris data
    with pytest.raises(SystemExit):
        batch_loader.quick_row_count_check([empty_file])


def test_quick_row_count_check_passes_with_data(tmp_path):
    """Pastikan fungsi lolos jika file CSV memiliki minimal 1 baris data valid."""
    f = tmp_path / "data.csv"
    f.write_text("col1,col2\nval1,val2\nval3,val4\n")
    # Tidak boleh melempar exception / SystemExit
    batch_loader.quick_row_count_check([f])
