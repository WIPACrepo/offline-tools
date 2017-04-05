
import iceprodinterface
from files import File

class IceProd1(iceprodinterface.IceProdInterface):
    def __init__(self, logger, dryrun):
        super(IceProd1, self).__init__(logger, dryrun)

        self._dbs4 = DatabaseConnection.get_connection('dbs4', logger)

    def _get_max_queue_id(self, dataset_id):
        """
        Retrieves the max queue id from the `i3filter.job` table.

        Args:
            dataset_id (int): The dataset id

        Returns:
            int: The max queue id from `i3filter.job`.
        """

        query = self._dbs4.fetchall("SELECT MAX(queue_id) AS `max_queue_id` FROM i3filter.job WHERE dataset_id = {dataset_id} ".format(dataset_id = dataset_id))
        return query[0]['max_queue_id']

    def submit_run(self, dataset_id, run, checksumcache):
        """
        Submits the run. It makes all the inserts to the database.

        Args:
            dataset_id (int): The dataset id
            run (runs.Run): The run
            checksumcache (files.ChecksumCache): The cache that manages the checksums
        """

        logger.info(run.format('IceProd1: Submitting run {run_id}, snapshot_id = {snapshot_id}, production_version = {production_version}'))

        path_prefix = 'gsiftp://gridftp.icecube.wisc.edu'

        logger.debug('Get PFFilt files')
        input_files = run.get_pffilt_files()
        gcd_file = run.get_gcd_file()
        gcd_checksum = None

        logger.debug("GCD file: {0}"format(GCDFileName))
        
        if gcd_file is not None:
            logger.debug('Calculate MD5 sum for gcd file')
            gcd_checksum = File.get_md5(gcd_file, logger)
        else:
            logger.critical("No GCD file found.")
            raise Exception('No GCD file found')
        
        if not len(input_files):
            logger.critical('No PFFilt files have been found')
            raise Exception('No input files found')
        else:
            queue_id = _get_max_queue_id(dataset_id)

            logger.debug('Last queue_id = {0}'.format(queue_id))

            logger.info("Attempting to submit {0} PFFilt Files for run {1}".format(len(input_files), run.run_id))
        
            for f in input_files:
                queue_id += 1

                sql = """
                        INSERT INTO i3filter.job (dataset_id, queue_id, status)
                        VALUES ({dataset_id}, {queue_id},"{status}")
                    """.format(dataset_id = dataset_id, queue_id = queue_id, status = 'WAITING')

                logger.debug('SQL: {0}'.format(sql))

                if not dryrun:
                    self._dbs4.execute(sql)

                sql = """
                        INSERT INTO i3filter.urlpath (dataset_id, queue_id, name, path, type, md5sum, size)
                        VALUES ({dataset_id}, {queue_id}, "{file_name}", "{file_path}", "INPUT", "{checksum}", {file_size})""".format(
                            dataset_id = dataset_id,
                            queue_id = queue_id,
                            file_name = os.path.basename(gcd_file),
                            file_path = path_prefix + os.path.dirname(gcd_file) + "/",
                            checksum = gcd_checksum,
                            file_size = os.path.getsize(gcd_file))

                logger.debug('SQL: {0}'.format(sql))

                if not dryrun:
                    self._dbs4.execute(sql)

                sql = """
                        INSERT INTO i3filter.urlpath (dataset_id, queue_id, name, path, type, md5sum, size)
                        VALUES ({dataset_id}, {queue_id}, "{file_name}", "{file_path}", "INPUT", "{checksum}", {file_size})""".format(
                            dataset_id = dataset_id,
                            queue_id = queue_id,
                            file_name = f.get_name(),
                            file_path = path_prefix + f.get_dirname() + "/",
                            checksum = f.md5(),
                            file_size = f.size())

                logger.debug('SQL: {0}'.format(sql))

                if not dryrun:
                    self._dbs4.execute(sql)

                sql = """
                    INSERT INTO i3filter.run (run_id, dataset_id, queue_id, sub_run, date)
                    VALUES ({run_id}, {dataset_id}, {queue_id}, {sub_run}, "{date}")""".format(
                        run_id = run.run_id,
                        dataset_id = dataset_id,
                        queue_id = queue_id,
                        sub_run = f.sub_run_id,
                        date = run.get_start_time().date_time.date())

                logger.debug('SQL: {0}'.format(sql))

                if not dryrun:
                    self._dbs4.execute(sql)

    def clean_run(self, dataset_id, run):
        query  = self._dbs4.fetchall(run.format("""
            SELECT j.queue_id
            FROM i3filter.job j
            JOIN i3filter.run r
                ON j.queue_id = r.queue_id AND
                    j.dataset_id = r.dataset_id
            WHERE r.dataset_id = {dataset_id} AND
                r.run_id = {run_id}""", dataset_id = dataset_id))

        queue_ids = [q['queue_id'] for q in query]

        # Clean job table
        sql = "DELETE FROM i3filter.job WHERE dataset_id = {dataset_id} AND queue_id IN ({queue_ids})".format(
            dataset_id = dataset_id,
            queue_ids = ','.join(queue_ids)
        )

        logger.debug('Cleaning SQL: {0}'.format(sql))

        if not self.dryrun:
             self._dbs4.execute(sql)

        # Clean urlpath table
        sql = "DELETE FROM i3filter.urlpath WHERE dataset_id = {dataset_id} AND queue_id IN ({queue_ids})".format(
            dataset_id = dataset_id,
            queue_ids = ','.join(queue_ids)
        )

        logger.debug('Cleaning SQL: {0}'.format(sql))

        if not self.dryrun:
             self._dbs4.execute(sql)

        # Clean run table
        sql = "DELETE FROM i3filter.run WHERE dataset_id = {dataset_id} AND queue_id IN ({queue_ids})".format(
            dataset_id = dataset_id,
            queue_ids = ','.join(queue_ids)
        )

        logger.debug('Cleaning SQL: {0}'.format(sql))

        if not self.dryrun:
             self._dbs4.execute(sql)

    def get_run_status(self):
        pass

    def is_run_submitted(self, dataset_id, run):
        sql = """
            SELECT * FROM i3filter.run r
            JOIN i3filter.job j
                ON r.dataset_id = j.dataset_id
                    AND r.queue_id = j.queue_id
            WHERE r.dataset_id = {dataset_id:d}
                AND run_id = {run_id:d}""".format(dataset_id = dataset_id, run_id = run.run_id)

        query = self._dbs4.fetchall(sql)
        return len(query) > 0



