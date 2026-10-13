"""Controlled vocabularies and their CLI validation helpers."""

import pytest

from sentinel_py.enums import CDSECollections, validate_product, validate_serial_id


def test_sentinel2_validation_uses_current_user_products_and_platforms():
    assert validate_product(CDSECollections.sentinel2, "s2msi1c") == "S2MSI1C"
    assert validate_product(CDSECollections.sentinel2, "s2msi2a") == "S2MSI2A"
    assert validate_serial_id(CDSECollections.sentinel2, "c") == "C"

    with pytest.raises(ValueError, match="S2MSI2B"):
        validate_product(CDSECollections.sentinel2, "S2MSI2B")
