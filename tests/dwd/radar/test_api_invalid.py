from datetime import datetime

import pytest

from wetterdienst.dwd.radar import DWDRadarData, RadarParameter
from wetterdienst.dwd.radar.metadata import (
    DWDRadarTimeResolution,
    DWDRadarPeriodType,
)
from wetterdienst.dwd.radar.metadata import RadarDate, RadarDataFormat
from wetterdienst.dwd.radar.sites import RadarSite
from wetterdienst.exceptions import InvalidEnumeration


def test_radar_request_site_historic_pe_wrong_parameters():
    """
    Verify acquisition of radar/site/PE_ECHO_TOP data croaks
    when omitting RadarDataFormat.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DWDRadarData(
            parameter=RadarParameter.PE_ECHO_TOP,
            site=RadarSite.BOO,
            start_date=datetime.utcnow(),
        )
        next(request.collect_data())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("Argument 'format' is missing")


def test_radar_request_site_historic_pe_future(caplog):
    """
    Verify that ``DWDRadarRequest`` will properly emit
    log messages when hitting empty results.

    This time for PE_ECHO_TOP data.
    """

    request = DWDRadarData(
        parameter=RadarParameter.PE_ECHO_TOP,
        site=RadarSite.BOO,
        format=RadarDataFormat.BUFR,
        start_date="2099-01-01 00:00:00",
    )
    results = list(request.collect_data())
    assert results == []

    assert "WARNING" in caplog.text
    assert "No radar file found" in caplog.text


def test_radar_request_site_latest_sweep_pcp_v_hdf5():
    """
    Verify requesting latest HDF5 data croaks.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DWDRadarData(
            parameter=RadarParameter.SWEEP_PCP_VELOCITY_H,
            site=RadarSite.BOO,
            format=RadarDataFormat.HDF5,
            start_date=RadarDate.LATEST,
        )

        list(request.collect_data())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("HDF5 data has no '-latest-' files")


def test_radar_request_site_latest_sweep_pcp_v_hdf5_wrong_parameters():
    """
    Verify requesting HDF5 data without RadarDataFormat croaks.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DWDRadarData(
            parameter=RadarParameter.SWEEP_PCP_VELOCITY_H,
            site=RadarSite.BOO,
            start_date=RadarDate.CURRENT,
        )

        list(request.collect_data())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("Argument 'format' is missing")


def test_radar_request_site_without_site():
    """
    Verify requesting site data without site croaks.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DWDRadarData(
            parameter=RadarParameter.SWEEP_PCP_VELOCITY_H,
            start_date=RadarDate.LATEST,
        )

        list(request.collect_data())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("Argument 'site' is missing")


def test_radar_request_hdf5_without_subset():
    """
    Verify requesting HDF5 data without "subset" croaks.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DWDRadarData(
            parameter=RadarParameter.SWEEP_PCP_VELOCITY_H,
            site=RadarSite.BOO,
            format=RadarDataFormat.HDF5,
            start_date=RadarDate.MOST_RECENT,
        )

        list(request.collect_data())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("Argument 'subset' is missing")


@pytest.mark.remote
@pytest.mark.parametrize(
    "time_resolution",
    [
        DWDRadarTimeResolution.DAILY,
        DWDRadarTimeResolution.HOURLY,
        DWDRadarTimeResolution.MINUTE_5,
    ],
)
def test_radar_request_radolan_cdc_latest(time_resolution):
    """
    Verify requesting latest RADOLAN_CDC croaks.
    """

    with pytest.raises(ValueError) as excinfo:
        request = DWDRadarData(
            parameter=RadarParameter.RADOLAN_CDC,
            time_resolution=time_resolution,
            start_date=RadarDate.LATEST,
        )

        list(request.collect_data())

    assert excinfo.typename == "ValueError"
    assert str(excinfo.value).startswith("RADOLAN_CDC data has no '-latest-' files")


def test_radar_request_radolan_cdc_invalid_time_resolution():
    """
    Verify requesting 1-minute RADOLAN_CDC croaks.
    """

    with pytest.raises(InvalidEnumeration):
        DWDRadarData(
            parameter=RadarParameter.RADOLAN_CDC,
            time_resolution="minute_1",
            period_type=DWDRadarPeriodType.RECENT,
            start_date="2019-08-08 00:50:00",
        )


# def test_radar_request_radolan_cdc_future(caplog):
#     """
#     Verify that ``DWDRadarRequest`` will properly emit
#     log messages when hitting empty results.
#
#     This time for RADOLAN_CDC data.
#     """
#     # with pytest.raises(ValueError):
#     request = DWDRadarData(
#         parameter=RadarParameter.RADOLAN_CDC,
#         time_resolution=DWDRadarTimeResolution.DAILY,
#         period_type=DWDRadarPeriodType.RECENT,
#         start_date="2099-01-01 00:50:00",
#     )
#
#     # results = list(request.collect_data())
#     # assert results == []
#     #
#     # assert "WARNING" in caplog.text
#     # assert "No radar file found" in caplog.text
