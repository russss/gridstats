from push_listener import unit_from_subject


def test_unit_from_subject():
    assert unit_from_subject("BMRA.DYNAMIC.T_MEDP-1.SEL") == "T_MEDP-1"
    assert unit_from_subject("BMRA.DYNAMIC.T_PEMB-31.RDRE") == "T_PEMB-31"
    assert unit_from_subject("BMRA.BM.E_ABERDARE.FPN") == "E_ABERDARE"
