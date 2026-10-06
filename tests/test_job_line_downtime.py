from mfg_lake.jobs.line_downtime_daily import shift_code_for_hour


def test_shift_3x8():
    assert shift_code_for_hour(6, "3x8") == "S1"
    assert shift_code_for_hour(13, "3x8") == "S1"
    assert shift_code_for_hour(14, "3x8") == "S2"
    assert shift_code_for_hour(22, "3x8") == "S3"
    assert shift_code_for_hour(3, "3x8") == "S3"


def test_shift_2x12():
    assert shift_code_for_hour(6, "2x12") == "D"
    assert shift_code_for_hour(18, "2x12") == "N"
    assert shift_code_for_hour(2, "2x12") == "N"
