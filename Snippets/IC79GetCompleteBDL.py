
def GetCompleteBDL(detector_frame):
    bdl = detector_frame['BadDomsList']

    # Unconfigured DOMs
    for dom in detector_frame['I3Geometry'].omgeo.keys():
        if dom not in detector_frame['I3DetectorStatus'].dom_status.keys() or \
           detector_frame['I3DetectorStatus'].dom_status[dom].pmt_hv == 0:
            bdl.append(dom)

    # No-HV DOMs
    for dom in detector_frame['I3Geometry'].omgeo.keys():
        if dom in detector_frame['I3DetectorStatus'].dom_status.keys() and \
           detector_frame['I3DetectorStatus'].dom_status[dom].pmt_hv == 0:
            bdl.append(dom)

    return set(sorted(bdl))


