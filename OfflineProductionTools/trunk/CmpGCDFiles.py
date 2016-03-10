#!/usr/bin/env python

import sys, os
from optparse import OptionParser
from math import isnan,fabs
import datetime
import itertools


try:
    from icecube import icetray, dataio, dataclasses
except Exception,err:
    print "\nMake sure I3Tray enviroment is enabled ...\n"
    raise Exception("Error: %s\n"%str(err))

def CheckFiles(GCDFiles):
    if not (isinstance(GCDFiles,list) or isinstance(GCDFiles,tuple)):
        print "***A list or tuple of 2 GCD files to be compared is required***"
        sys.exit(1)
    if not len(GCDFiles) == 2:
        print "***Must supply names of 2 GCD files to compare***"
        sys.exit(1)
    if not (os.path.isfile(GCDFiles[0]) and os.path.isfile(GCDFiles[1])):
        print "***One or both GCD Files don't exist, check the file path ..."
        sys.exit(1)
    return (0)

def GetGCDObject(i3file,ObjectName):
    f = dataio.I3File(i3file)
    while f.more():
        GCD = f.pop_frame()
        if GCD.Has(ObjectName):
            return GCD[ObjectName]
    
    if ObjectName =="BadDomsList": return []

    print "***No %s frame in file %s***"%(ObjectName,i3file)
    sys.exit(1)

    
def PrintVerboseDifference(GCDFiles,Keys1,Keys2):
    tmp_diff = list(set(Keys1) - set(Keys2))
    if len(tmp_diff):
        tmp_diff.sort()
        print "entries in %s but not in %s"%(GCDFiles[0],GCDFiles[1])
        for t_d in tmp_diff:
            print t_d
    tmp_diff = list(set(Keys2) - set(Keys1))
    if len(tmp_diff):
        tmp_diff.sort()
        print "entries in %s but not in %s"%(GCDFiles[1],GCDFiles[0])
        for t_d in tmp_diff:
            print t_d


def CmpCalibration(GCDFiles,BadDOMsList=[],V=False,T=False):
    
    print "\nComparing Calibration ......"

    checkFail = 0

    CheckFiles(GCDFiles)

    C1 = GetGCDObject(GCDFiles[0],'I3Calibration')
    C2 = GetGCDObject(GCDFiles[1],'I3Calibration')
    
    # Check DOM Calibrations
    DC1 = C1.dom_cal
    DC2 = C2.dom_cal
    
    D_Key1 = DC1.keys()
    D_Key2 = DC2.keys()
    

    #if len(list(set(D_Key1) - set(D_Key2))) or len(list(set(D_Key2) - set(D_Key1))):
    if len([ld for ld in list(set(D_Key1) - set(D_Key2))+list(set(D_Key2) - set(D_Key1)) if ld not in BadDOMsList]):
        checkFail = 1    
        print "Different number of entries for DOM calibration"
        if V:
            PrintVerboseDifference(GCDFiles,D_Key1,D_Key2)
        #return(1)
    
    DCal_Diff_Dict = {}
    
    #D_Key1 = D_Key1[0:1]
    for k in D_Key1:
        if k in BadDOMsList: continue
        
        if k not in D_Key2: continue

        DCal_Diff_Tdict = {}
        
        #atwd_gain_Check = False not in [DC1[k].atwd_gain[i]==DC2[k].atwd_gain[i]for i in xrange(3)]
        #atwd_gain_Check = all([DC1[k].atwd_gain[i]==DC2[k].atwd_gain[i] for i in xrange(3)])
        DCal_Diff_Tdict['atwd_gain_Check'] = all([DC1[k].atwd_gain[i]==DC2[k].atwd_gain[i] for i in xrange(3)])
         
        #atwd_delta_t_Check = False not in [DC1[k].atwd_delta_t[i]==DC2[k].atwd_delta_t[i]for i in xrange(2)]
        DCal_Diff_Tdict['atwd_delta_t_Check'] = all([DC1[k].atwd_delta_t[i]==DC2[k].atwd_delta_t[i]for i in xrange(2)])
        
        #atwd_freq_fit_Check = list(itertools.chain.from_iterable([[DC1[k].atwd_freq_fit[i].quad_fit_a,DC1[k].atwd_freq_fit[i].quad_fit_b,DC1[k].atwd_freq_fit[i].quad_fit_v] for i in xrange(2)]))\
        #                      == list(itertools.chain.from_iterable([[DC2[k].atwd_freq_fit[i].quad_fit_a,DC2[k].atwd_freq_fit[i].quad_fit_b,DC2[k].atwd_freq_fit[i].quad_fit_v] for i in xrange(2)]))
        DCal_Diff_Tdict['atwd_freq_fit_Check'] = list(itertools.chain.from_iterable([[DC1[k].atwd_freq_fit[i].quad_fit_a,DC1[k].atwd_freq_fit[i].quad_fit_b,DC1[k].atwd_freq_fit[i].quad_fit_v] for i in xrange(2)]))\
                                                == list(itertools.chain.from_iterable([[DC2[k].atwd_freq_fit[i].quad_fit_a,DC2[k].atwd_freq_fit[i].quad_fit_b,DC2[k].atwd_freq_fit[i].quad_fit_v] for i in xrange(2)]))
    
        
        # only in old GCD
        #DCal_Diff_Tdict['tau_params_Check'] = [DC1[k].tau_parameters.p0,DC1[k].tau_parameters.p1,DC1[k].tau_parameters.p2,DC1[k].tau_parameters.p3,DC1[k].tau_parameters.p4,DC1[k].tau_parameters.p5,DC1[k].tau_parameters.tau_frac] \
        #                                      == [DC2[k].tau_parameters.p0,DC2[k].tau_parameters.p1,DC2[k].tau_parameters.p2,DC2[k].tau_parameters.p3,DC2[k].tau_parameters.p4,DC2[k].tau_parameters.p5,DC2[k].tau_parameters.tau_frac]
         
        # only in old GCD
        #DCal_Diff_Tdict['atwd_bin_calib_fit_Check'] = list(itertools.chain.from_iterable([[DC1[k].atwd_bin_calib_fit[a,b,c].intercept,DC1[k].atwd_bin_calib_fit[a,b,c].slope] for a in xrange(2) for b in xrange(3) for c in xrange(128)])) \
        #                            == list(itertools.chain.from_iterable([[DC2[k].atwd_bin_calib_fit[a,b,c].intercept,DC2[k].atwd_bin_calib_fit[a,b,c].slope] for a in xrange(2) for b in xrange(3) for c in xrange(128)]))
        
        # only in old GCD
        #DCal_Diff_Tdict['atwd_baseline_Check'] = [DC1[k].atwd_baseline[a,b,c] for a in xrange(2) for b in xrange(3) for c in xrange(128)] \
        #                    == [DC2[k].atwd_baseline[a,b,c] for a in xrange(2) for b in xrange(3) for c in xrange(128)]
        
        #atwd_beacon_baseline_Check = [DC1[k].atwd_beacon_baseline[a,b] for a in xrange(2) for b in xrange(3)] \
        #                           == [DC2[k].atwd_beacon_baseline[a,b] for a in xrange(2) for b in xrange(3)]
        DCal_Diff_Tdict['atwd_beacon_baseline_Check'] = [DC1[k].atwd_beacon_baseline[a,b] for a in xrange(2) for b in xrange(3)] \
                                   == [DC2[k].atwd_beacon_baseline[a,b] for a in xrange(2) for b in xrange(3)]
        #if not DCal_Diff_Tdict['atwd_beacon_baseline_Check']:
        #    print [100*fabs(DC2[k].atwd_beacon_baseline[a,b] - DC1[k].atwd_beacon_baseline[a,b])/DC1[k].atwd_beacon_baseline[a,b] for a in xrange(2) for b in xrange(3) if DC1[k].atwd_beacon_baseline[a,b]!=0]
        
        
        DCal_Diff_Tdict['dom_cal_version_Check'] = DC1[k].dom_cal_version==DC2[k].dom_cal_version
        
        DCal_Diff_Tdict['dom_noise_rate_Check'] = DC1[k].dom_noise_rate==DC2[k].dom_noise_rate
        
        DCal_Diff_Tdict['relative_dom_eff_Check'] = DC1[k].relative_dom_eff==DC2[k].relative_dom_eff
        
        DCal_Diff_Tdict['temperature_Check'] = DC1[k].temperature==DC2[k].temperature
        
        DCal_Diff_Tdict['transit_time_Check'] = [DC1[k].transit_time.intercept,DC1[k].transit_time.slope]==[DC2[k].transit_time.intercept,DC2[k].transit_time.slope]
        
        DCal_Diff_Tdict['hv_gain_fit_Check'] = [DC1[k].hv_gain_fit.intercept,DC1[k].hv_gain_fit.slope]==[DC2[k].hv_gain_fit.intercept,DC2[k].hv_gain_fit.slope]
           
        DCal_Diff_Tdict['spe_disc_calib_Check'] =  [DC1[k].spe_disc_calib.intercept,DC1[k].spe_disc_calib.slope]==[DC2[k].spe_disc_calib.intercept,DC2[k].spe_disc_calib.slope]  
        
        DCal_Diff_Tdict['mpe_disc_calib_Check'] =  [DC1[k].mpe_disc_calib.intercept,DC1[k].mpe_disc_calib.slope]==[DC2[k].mpe_disc_calib.intercept,DC2[k].mpe_disc_calib.slope]  
        
        DCal_Diff_Tdict['pmt_disc_calib_Check'] =  [DC1[k].pmt_disc_calib.intercept,DC1[k].pmt_disc_calib.slope]==[DC2[k].pmt_disc_calib.intercept,DC2[k].pmt_disc_calib.slope]  
        
        DCal_Diff_Tdict['front_end_impedance_Check'] = DC1[k].front_end_impedance==DC2[k].front_end_impedance
          
        DCal_Diff_Tdict['fadc_baseline_fit_Check'] =  [DC1[k].fadc_baseline_fit.intercept,DC1[k].fadc_baseline_fit.slope]==[DC2[k].fadc_baseline_fit.intercept,DC2[k].fadc_baseline_fit.slope]
        
        DCal_Diff_Tdict['fadc_gain_Check'] = DC1[k].fadc_gain==DC2[k].fadc_gain
        
        DCal_Diff_Tdict['fadc_beacon_baseline_Check'] = DC1[k].fadc_beacon_baseline==DC2[k].fadc_beacon_baseline
        #if not DCal_Diff_Tdict['fadc_beacon_baseline_Check'] :
        #    print 100* fabs(DC2[k].fadc_beacon_baseline - DC1[k].fadc_beacon_baseline)/DC1[k].fadc_beacon_baseline
        
        DCal_Diff_Tdict['fadc_delta_t_Check'] = DC1[k].fadc_delta_t==DC2[k].fadc_delta_t
        
        # only in old GCD
        #DCal_Diff_Tdict['fadc_response_width_Check'] = DC1[k].fadc_response_width==DC2[k].fadc_response_width
        
        # only in old GCD
        #DCal_Diff_Tdict['atwd_response_width_Check'] = DC1[k].atwd_response_width==DC2[k].atwd_response_width
                       
        
        if not all(DCal_Diff_Tdict.values()):
            DCal_Diff_Dict[k] = [i for i in DCal_Diff_Tdict.keys() if not DCal_Diff_Tdict[i]]
    
    if len(DCal_Diff_Dict):
        checkFail = 1
        print "\nThe DOM Calibration entries for the GCD files %s differ"%(" , ".join(GCDFiles))
        if (V):
            print "\nCalibration entries for the following (%s) DOMs-Properties differ:"%len(DCal_Diff_Dict)
            sk_DCal_Diff_Dict = DCal_Diff_Dict.keys()
            sk_DCal_Diff_Dict.sort()
            #for ddd in DCal_Diff_Dict.keys() :
            for ddd in sk_DCal_Diff_Dict:
                print ddd,DCal_Diff_Dict[ddd]
    
    
    # Check VEM Cal
    VC1 = C1.vem_cal
    VC2 = C2.vem_cal
    
    V_Key1 = VC1.keys()
    V_Key2 = VC2.keys()
    
    #if len(list(set(V_Key1) - set(V_Key2))) or len(list(set(V_Key2) - set(V_Key1))):
    if len([vd for vd in list(set(V_Key1) - set(V_Key2))+list(set(V_Key2) - set(V_Key1)) if ld not in BadDOMsList]):
        checkFail = 1
        print "Different number of VEM Calibrations entries "
        if V:
            PrintVerboseDifference(GCDFiles,V_Key1,V_Key2)
        return(1)
    
    VCal_Diff = []
    
    for k in V_Key1:
        if k in BadDOMsList: continue
        
        if not [VC1[k].pe_per_vem,VC1[k].mu_peak_width,VC1[k].hglg_cross_over,VC1[k].corr_factor] \
              == [VC2[k].pe_per_vem,VC2[k].mu_peak_width,VC2[k].hglg_cross_over,VC2[k].corr_factor] :
            
            VCal_Diff.append(k)
            
    if len(VCal_Diff):
        checkFail = 1
        print "\nVEM Cal. entries for the GCD files %s differ"%(" , ".join(GCDFiles))
        if (V):
            print "\nVEM Cal. entries for the following DOMs differ:"
            for v_c in VCal_Diff:
                print v_c
        
    if not(C1.start_time == C2.start_time):
        #checkFail = 1
        print "\nThe DOM Calibration start times in the GCDFiles %s differ"%(" , ".join(GCDFiles))
        if V:
            print 'Start time for: ',GCDFiles[0],C1.start_time
            print 'Start time for: ',GCDFiles[1],C2.start_time
    
    if not(C1.end_time == C2.end_time):
        #checkFail = 1
        print "\nThe DOM Calibration end times in the GCDFiles %s differ"%(" , ".join(GCDFiles))
        if V:
            print 'End time for: ',GCDFiles[0],C1.end_time
            print 'End time for: ',GCDFiles[1],C2.end_time

    return checkFail

def CmpDetectorStatus(GCDFiles,BadDOMsList=[],V=False,T=False):
    print "\nComparing Detector status ......"
    
    checkFail = 0
    
    CheckFiles(GCDFiles)

    D1 = GetGCDObject(GCDFiles[0],'I3DetectorStatus')
    D2 = GetGCDObject(GCDFiles[1],'I3DetectorStatus')
    
    # Check DOM status
    DS1 = D1.dom_status
    DS2 = D2.dom_status
    
    D_Key1 = DS1.keys()
    D_Key2 = DS2.keys()
    
    
    #if len(list(set(D_Key1) - set(D_Key2))) or len(list(set(D_Key2) - set(D_Key1))):
    if len([ld for ld in list(set(D_Key1) - set(D_Key2))+list(set(D_Key2) - set(D_Key1)) if ld not in BadDOMsList]):
        checkFail = 1
        print "Different number of entries for DOM status"
        if V:
            PrintVerboseDifference(GCDFiles,D_Key1,D_Key2)
        return(1)
    
    DStatus_Diff = []
    DStatus_Diff_Dict = {}
    
    #D_Key1 = D_Key1[0:1]
    for k in D_Key1:
        if k in BadDOMsList: continue
        
        DStatus_Diff_Tdict = {}
        
        DStatus_Diff_Tdict['trig_mode_Check'] = DS1[k].trig_mode.numerator==DS2[k].trig_mode.numerator
        DStatus_Diff_Tdict['lc_mode_Check'] = DS1[k].lc_mode.numerator==DS2[k].lc_mode.numerator
        DStatus_Diff_Tdict['lc_window_pre_Check'] = DS1[k].lc_window_pre==DS2[k].lc_window_pre
        DStatus_Diff_Tdict['lc_window_post_Check'] = DS1[k].lc_window_post==DS2[k].lc_window_post
        DStatus_Diff_Tdict['lc_span_Check'] = DS1[k].lc_span==DS2[k].lc_span
        DStatus_Diff_Tdict['status_atwd_a_Check'] = DS1[k].status_atwd_a.numerator==DS2[k].status_atwd_a.numerator
        DStatus_Diff_Tdict['status_atwd_b_Check'] = DS1[k].status_atwd_b.numerator==DS2[k].status_atwd_b.numerator
        DStatus_Diff_Tdict['status_fadc_Check'] = DS1[k].status_fadc.numerator==DS2[k].status_fadc.numerator
        DStatus_Diff_Tdict['pmt_hv_Check'] = DS1[k].pmt_hv==DS2[k].pmt_hv
        DStatus_Diff_Tdict['spe_threshold_Check'] = DS1[k].spe_threshold==DS2[k].spe_threshold
        DStatus_Diff_Tdict['fe_pedestal_Check'] = DS1[k].fe_pedestal==DS2[k].fe_pedestal
        DStatus_Diff_Tdict['dac_trigger_bias_0_Check'] = DS1[k].dac_trigger_bias_0==DS2[k].dac_trigger_bias_0
        DStatus_Diff_Tdict['dac_trigger_bias_1_Check'] = DS1[k].dac_trigger_bias_1==DS2[k].dac_trigger_bias_1
        DStatus_Diff_Tdict['dac_fadc_ref_Check'] = DS1[k].dac_fadc_ref==DS2[k].dac_fadc_ref
        DStatus_Diff_Tdict['dom_gain_type_Check'] = DS1[k].dom_gain_type.numerator==DS2[k].dom_gain_type.numerator
        DStatus_Diff_Tdict['cable_type_Check'] = DS1[k].cable_type.numerator==DS2[k].cable_type.numerator
        DStatus_Diff_Tdict['delta_compress_Check'] = DS1[k].delta_compress.numerator==DS2[k].delta_compress.numerator
        DStatus_Diff_Tdict['slc_active_Check'] = DS1[k].slc_active==DS2[k].slc_active
        DStatus_Diff_Tdict['mpe_threshold_Check'] = DS1[k].mpe_threshold==DS2[k].mpe_threshold
        DStatus_Diff_Tdict['tx_mode_Check'] = DS1[k].tx_mode.numerator==DS2[k].tx_mode.numerator
        
            
        if not all(DStatus_Diff_Tdict.values()):
            #DStatus_Diff_Dict[k] = [i for i in DCal_Status_Tdict.keys() if not DCal_Status_Tdict[i]]
            DStatus_Diff_Dict[k] = [i for i in DStatus_Diff_Tdict.keys() if not DStatus_Diff_Tdict[i]]
            

    if len(DStatus_Diff_Dict):
        checkFail = 1
        print "\nThe Detector status entries for the GCD files %s differ"%(" , ".join(GCDFiles))
        if (V):
            sk_DStatus_Diff_Dict = DStatus_Diff_Dict.keys()
            sk_DStatus_Diff_Dict.sort()

            print "\nDetector status entries for the following %s DOMs-Objects differ:"%len(DStatus_Diff_Dict)
            #for dds in DStatus_Diff_Dict.keys() :
            for dds in sk_DStatus_Diff_Dict:
                print dds,DStatus_Diff_Dict[dds]
    
    # Check Trigger status
    TS1 = D1.trigger_status
    TS2 = D2.trigger_status
    
    T_Key1 = TS1.keys()
    T_Key2 = TS2.keys()
    
    T_types1 = [k.type for k in T_Key1]
    T_types2 = [k.type for k in T_Key2]
    
    
    if len(list(set(T_types1) - set(T_types2))) or len(list(set(T_types2) - set(T_types1))):
        checkFail = 1
        print "Different number of entries for Trigger status"
        if V:
            PrintVerboseDifference(GCDFiles,T_types1,T_types2)
        return(1)
    
    TStatus_Diff = []
    
    
    for tk in T_Key1:
        try:
            tr_settings = [TS1[tk].trigger_settings[kr]==TS2[tk].trigger_settings[kr] for kr in TS1[tk].trigger_settings.keys()]
            tr_settings_check = all(tr_settings)
        except:
            tr_settings_check = False
        
        try:
            tr_readoutWindow = [[TS1[tk].readout_settings[tw].readout_time_minus == TS2[tk].readout_settings[tw].readout_time_minus, \
                                   TS1[tk].readout_settings[tw].readout_time_plus == TS2[tk].readout_settings[tw].readout_time_plus, \
                                   TS1[tk].readout_settings[tw].readout_time_offset == TS2[tk].readout_settings[tw].readout_time_offset]\
                                   for tw in TS1[tk].readout_settings.keys()]

            # flatten list of lists into list sum(tr_readoutWindow,[])
            #tr_readoutWindow_check = False not in sum(tr_readoutWindow,[])
            tr_readoutWindow_check = all(itertools.chain.from_iterable(tr_readoutWindow))          
            
        except:
            tr_readoutWindow_check = False

        if not (TS1[tk].trigger_name == TS2[tk].trigger_name and
                tr_settings_check and
                tr_readoutWindow_check):
            
            TStatus_Diff.append(tk) 
    
    if len(TStatus_Diff):
        checkFail = 1
        print "\nThe Trigger status entries for the GCD files %s differ"%(" , ".join(GCDFiles))
        if (V):
            print "\nThe following Trigger status entries differ:"
            for t_s in TStatus_Diff:
                print t_s
        
    if not(D1.start_time == D2.start_time):
        # The start times should be the same, but in practice can also be off by a few seconds
        #if not T: checkFail = 1
        print "\nThe start times in the Detector status object of the GCDFiles %s differ"%(" , ".join(GCDFiles))
        if V:
            print 'Start time for: ',GCDFiles[0],D1.start_time
            print 'Start time for: ',GCDFiles[1],D2.start_time
    
    if not(D1.end_time == D2.end_time):
        # these are different because the end time cannot be determined at the start of the run so it is set to some default later date
        #checkFail = 1
        print "\nThe end times in the Detector status object of the GCDFiles %s differ"%(" , ".join(GCDFiles))
        if V:
            print 'End time for: ',GCDFiles[0],D1.end_time
            print 'End time for: ',GCDFiles[1],D2.end_time
    
    
    return checkFail
    
#def CmpGeometry(GCDFiles,G1,G2,V='false'):
def CmpGeometry(GCDFiles,BadDOMsList=[],V=False,T=False):
    
    print "\nComparing Geometry ......"

    checkFail = 0

    CheckFiles(GCDFiles)

    G1 = GetGCDObject(GCDFiles[0],'I3Geometry')
    G2 = GetGCDObject(GCDFiles[1],'I3Geometry')
    
    # Check OM Geometry
    OMGeo1 = G1.omgeo
    OMGeo2 = G2.omgeo
    
    O_Key1 = OMGeo1.keys()
    O_Key2 = OMGeo2.keys()

    #if len(list(set(O_Key1) - set(O_Key2))) or len(list(set(O_Key2) - set(O_Key1))):
    if len([od for od in list(set(O_Key1) - set(O_Key2))+list(set(O_Key2) - set(O_Key1)) if od not in BadDOMsList]):
        checkFail = 1
        print "Different number of entries for OM Geometry"
        if V:
            PrintVerboseDifference(GCDFiles,O_Key1,O_Key2)
        return(1)
    
    OMGeo_Diff_Dict = {}
    
    #O_Key1 = O_Key1[0:1]
    for k in O_Key1:
        if k in BadDOMsList: continue
        
        OMGeo_Diff_Tdict = {}
        
        OMGeo_Diff_Tdict['position_x_Check'] = OMGeo1[k].position.x==OMGeo2[k].position.x
        OMGeo_Diff_Tdict['position_y_Check'] = OMGeo1[k].position.y==OMGeo2[k].position.y
        OMGeo_Diff_Tdict['position_z_Check'] = OMGeo1[k].position.z==OMGeo2[k].position.z
        OMGeo_Diff_Tdict['orientation_dir_azimuth_Check'] = OMGeo1[k].orientation.dir_azimuth==OMGeo2[k].orientation.dir_azimuth
        OMGeo_Diff_Tdict['orientation_dir_phi_Check'] = OMGeo1[k].orientation.dir_phi==OMGeo2[k].orientation.dir_phi
        OMGeo_Diff_Tdict['orientation_dir_theta_Check'] = OMGeo1[k].orientation.dir_theta==OMGeo2[k].orientation.dir_theta
        OMGeo_Diff_Tdict['orientation_dir_zenith_Check'] = OMGeo1[k].orientation.dir_zenith==OMGeo2[k].orientation.dir_zenith
        OMGeo_Diff_Tdict['orientation_dir_x_Check'] = OMGeo1[k].orientation.dir_x==OMGeo2[k].orientation.dir_x
        OMGeo_Diff_Tdict['orientation_dir_y_Check'] = OMGeo1[k].orientation.dir_y==OMGeo2[k].orientation.dir_y
        OMGeo_Diff_Tdict['orientation_dir_z_Check'] = OMGeo1[k].orientation.dir_z==OMGeo2[k].orientation.dir_z
        OMGeo_Diff_Tdict['orientation_up_azimuth_Check'] = OMGeo1[k].orientation.up_azimuth==OMGeo2[k].orientation.up_azimuth
        OMGeo_Diff_Tdict['orientation_up_phi_Check'] = OMGeo1[k].orientation.up_phi==OMGeo2[k].orientation.up_phi
        OMGeo_Diff_Tdict['orientation_up_theta_Check'] = OMGeo1[k].orientation.up_theta==OMGeo2[k].orientation.up_theta
        OMGeo_Diff_Tdict['orientation_up_zenith_Check'] = OMGeo1[k].orientation.up_zenith==OMGeo2[k].orientation.up_zenith
        OMGeo_Diff_Tdict['orientation_up_x_Check'] = OMGeo1[k].orientation.up_x==OMGeo2[k].orientation.up_x
        OMGeo_Diff_Tdict['orientation_up_y_Check'] = OMGeo1[k].orientation.up_y==OMGeo2[k].orientation.up_y
        OMGeo_Diff_Tdict['orientation_up_z_Check'] = OMGeo1[k].orientation.up_z==OMGeo2[k].orientation.up_z
        OMGeo_Diff_Tdict['orientation_right_azimuth_Check'] = OMGeo1[k].orientation.right_azimuth==OMGeo2[k].orientation.right_azimuth
        OMGeo_Diff_Tdict['orientation_right_phi_Check'] = OMGeo1[k].orientation.right_phi==OMGeo2[k].orientation.right_phi
        OMGeo_Diff_Tdict['orientation_right_theta_Check'] = OMGeo1[k].orientation.right_theta==OMGeo2[k].orientation.right_theta
        OMGeo_Diff_Tdict['orientation_right_zenith_Check'] = OMGeo1[k].orientation.right_zenith==OMGeo2[k].orientation.right_zenith
        OMGeo_Diff_Tdict['orientation_right_x_Check'] = OMGeo1[k].orientation.right_x==OMGeo2[k].orientation.right_x
        OMGeo_Diff_Tdict['orientation_right_y_Check'] = OMGeo1[k].orientation.right_y==OMGeo2[k].orientation.right_y
        OMGeo_Diff_Tdict['orientation_right_z_Check'] = OMGeo1[k].orientation.right_z==OMGeo2[k].orientation.right_z
        OMGeo_Diff_Tdict['omtype_Check'] = OMGeo1[k].omtype.denominator==OMGeo2[k].omtype.denominator
        OMGeo_Diff_Tdict['area_Check'] = OMGeo1[k].area==OMGeo2[k].area
        
    
        if not all(OMGeo_Diff_Tdict):
            OMGeo_Diff_Dict[k] = [i for i in OMGeo_Diff_Tdict.keys() if not OMGeo_Diff_Tdict.values()]
            
            
    if len(OMGeo_Diff_Dict):
        checkFail = 1
        print "\nThe DOM geomteries for the GCD files %s differ"%(" , ".join(GCDFiles))
        if (V):
            print "\nGeometries for the following DOM-Property differ:"
            for omd in OMGeo_Diff_Dict:
                print omd,"-",OMGeo_Diff_Dict[omd]
    
    # Check IceTop Station Geometry
    SGeo1 = G1.stationgeo
    SGeo2 = G2.stationgeo
    
    S_Key1 = SGeo1.keys()
    S_Key2 = SGeo2.keys()
    
    if len(list(set(S_Key1) - set(S_Key2))) or len(list(set(S_Key2) - set(S_Key1))):
        checkFail = 1
        print "Different number of entries for Station Geometries"
        if V:
            PrintVerboseDifference(GCDFiles,S_Key1,S_Key2)
        return(1)
    
    StationGeo_Diff = []
    StationGeo_Diff_Dict = {}
    
    
    for ks in S_Key1:
        for r in xrange(len(SGeo1[ks])):
            
            StationGeo_Diff_Tdict = {}
            
            StationGeo_Diff_Tdict['position_x_Check'] = SGeo1[ks][r].position.x == SGeo2[ks][r].position.x
            StationGeo_Diff_Tdict['position_y_Check'] = SGeo1[ks][r].position.y == SGeo2[ks][r].position.y
            StationGeo_Diff_Tdict['position_z_Check'] = SGeo1[ks][r].position.z == SGeo2[ks][r].position.z
            #StationGeo_Diff_Tdict['orientation_Check'] = SGeo1[ks][r].orientation == SGeo2[ks][r].orientation
            StationGeo_Diff_Tdict['tankradius_Check'] = SGeo1[ks][r].tankradius == SGeo2[ks][r].tankradius
            StationGeo_Diff_Tdict['tankheight_Check'] = SGeo1[ks][r].tankheight == SGeo2[ks][r].tankheight
            StationGeo_Diff_Tdict['fillheight_Check'] = SGeo1[ks][r].fillheight == SGeo2[ks][r].fillheight
            StationGeo_Diff_Tdict['snowheight_Check'] = SGeo1[ks][r].snowheight == SGeo2[ks][r].snowheight
            StationGeo_Diff_Tdict['omkeylist_Check'] = set([k for k in SGeo1[ks][r].omkey_list]) == set([k for k in SGeo2[ks][r].omkey_list])
            
            if not all(StationGeo_Diff_Tdict.values()):
                StationGeo_Diff_Dict[ks] = [i for i in StationGeo_Diff_Tdict.keys() if not StationGeo_Diff_Tdict[i]]                                
            
    if len(StationGeo_Diff_Dict):
        checkFail = 1
        print "\nThe Icetop station geomteries for the GCD files %s differ"%(" , ".join(GCDFiles))
        if (V):
            print "\nGeometries for the following Icetop Station-Properties differ:"
            for sdd in StationGeo_Diff_Dict:
                print sdd,"-",StationGeo_Diff_Dict[sdd]
    
    
    if not(G1.start_time == G2.start_time):
        #checkFail = 1
        print "\nThe start times in the Geometry object of the GCDFiles %s differ"%(" , ".join(GCDFiles))
        if V:
            print 'Start time for: ',GCDFiles[0],G1.start_time
            print 'Start time for: ',GCDFiles[1],G2.start_time
    
    if not(G1.end_time == G2.end_time):
        #checkFail = 1
        print "\nThe end times in the Geometry object of the GCDFiles %s differ"%(" , ".join(GCDFiles))
        if V:
            print 'End time for: ',GCDFiles[0],G1.end_time
            print 'End time for: ',GCDFiles[1],G2.end_time
    
    return checkFail


if __name__ == '__main__':
    
    if len(sys.argv) < 3:
        raise RuntimeError("you have to enter 2 GCD files to compare")
    
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    
    parser.add_option("-f", "--gcdfiles", action="store",
                        type="string", default="", dest="gcdfiles", nargs=2,
                        help=""" Input GCD files to be compared  (full path to 2 GCD files separated by a space).
                                 The order matters, it is assumed that the GCD file containing a BadDOMsList is listed first
                                 Typically, only GCD files generated in the North contain BadDOMsLists """)
    
    parser.add_option("-v", "--verbose", action="store_true", default=False,
              dest="VERBOSE", help="print out explicit differences in GCD files")
    
    parser.add_option("-t", "--template", action="store_true", default=False,
              dest="TEMPLATE", help="""use this option for comparing GCD files to previous GCD files.
                                       It is expected that the validity times will change but nothing else
                                       except a calibration insertion is done.""")
    
    #-----------------------------------------------------------------
    # Parse cmd line args, exit if anything is not understood
    #-----------------------------------------------------------------
    
    (options,args) = parser.parse_args()
    if len(args) != 0:
        message = "Got undefined options:"
        for a in args:
            message += a
            message += " "
        parser.error(message)
    
    GCDFiles = options.gcdfiles
    print "comparing 2 GCD files: ",GCDFiles
    
    
    V = options.VERBOSE
    
    T = options.TEMPLATE
    
    #t = datetime.datetime.now()
    
    BadDOMsList = []
    try:
        BadDOMsList1 = GetGCDObject(GCDFiles[0],'BadDomsList')
    except:
        BadDOMsList1 = []
    try:
        BadDOMsList2 = GetGCDObject(GCDFiles[1],'BadDomsList')
    except:
        BadDOMsList2 = []
    
    #if len(BadDOMsList1) and len(BadDOMsList2):
    #    if set(BadDOMsList1) != set(BadDOMsList2):
    #        print """ Both GCD files contain BadDOMsLists and these lists differ
    #              any further comparison requires exclusion of DOMs in a list common to both GCD files
    #              please determine why these lists differ .... exiting"""
    #        
    #        print " DOMs in the BadDOMsList in %s but not in %s are:"%(GCDFiles[0],GCDFiles[1])
    #        tmp_diff = list(set(BadDOMsList1) - set(BadDOMsList2))
    #        if len(tmp_diff):
    #            for t in tmp_diff : print t
    #        print " DOMs in the BadDOMsList in %s but not in %s are:"%(GCDFiles[1],GCDFiles[0])
    #        tmp_diff = list(set(BadDOMsList2) - set(BadDOMsList1))
    #        if len(tmp_diff):
    #            for t in tmp_diff : print t    
    #            
    #        exit(1)
        
    BadDOMsList1.extend(BadDOMsList2)
    BadDOMsList = list(set(BadDOMsList1))
    
    checkGeometry = CmpGeometry(GCDFiles,BadDOMsList,V,T)
    print "Geometry check return: ", checkGeometry
    
    checkDetStatus = CmpDetectorStatus(GCDFiles,BadDOMsList,V,T)
    print "Detector Status check return: ", checkDetStatus
    
    #print "\nCombined Return Value: %d"%(checkGeometry+checkDetStatus)
    #sys.exit(checkGeometry+checkDetStatus)
    
    checkCalibration = CmpCalibration(GCDFiles,BadDOMsList,V,T)
    print "Calibration check return: ",checkCalibration
    print "\nCombined Return Value: %d"%(checkGeometry+checkDetStatus+checkCalibration)
    sys.exit(checkGeometry+checkDetStatus+checkCalibration)
    
    #print datetime.datetime.now() - t
    
    
