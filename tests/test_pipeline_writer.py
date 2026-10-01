import pytest

from sentinel_py.pipeline.writer import ArtifactOutputWriter, OutputWriterRegistry


def test_output_writer_registry_rejects_duplicate_formats():
    registry = OutputWriterRegistry()
    registry.register(ArtifactOutputWriter("example"))

    with pytest.raises(ValueError, match="already registered"):
        registry.register(ArtifactOutputWriter("example"))


def test_output_writer_registry_rejects_unknown_formats():
    registry = OutputWriterRegistry()

    with pytest.raises(KeyError, match="Unknown output format"):
        registry.get("absent")
