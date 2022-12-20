
from .files import File

def get_gcd_file(run, args, config, logger):
    if not args.cosmicray:
        # Just use the normal GCD file
        # Note: We don't want to use the GCD file from the L2 run folder (just in case
        # that the path will change or so).
        return run.get_gcd_file(exclude_run_folder_gcd = True)
    else:
        # The cosmic ray WG uses special GCD files
        cr_gcd_file = File(run.format(config.get('Level3', 'CosmicRayGCD')))

        if not cr_gcd_file.exists():
            logger.error('Did not find CosmicRay GCD file: {0}'.format(cr_gcd_file))
            return None
        else:
            return cr_gcd_file

def get_cosmicray_mc_gcd_file(season, config):
    mc_gcd_files = config.get_var_dict('Level3', 'CosmicRayMCGCD_', keytype = int)

    if season not in mc_gcd_files:
        return None
    else:
        return File(mc_gcd_files[season])


