
import os
import iceprodinterface

from files import File
from libs.databaseconnection import DatabaseConnection
from path import remove_path_prefix
from runs import SubRun

class IceProd1(iceprodinterface.IceProdInterface):
    def __init__(self, logger, dryrun):
        super(IceProd1, self).__init__(logger, dryrun)

        self._dbs4 = DatabaseConnection.get_connection('dbs4', logger)
        self.path_prefix = 'gsiftp://gridftp.icecube.wisc.edu'

    def _get_max_queue_id(self, dataset_id, run = None):
        """
        Retrieves the max queue id from the `i3filter.job` table.

        Args:
            dataset_id (int): The dataset id
            run (runs.Run): If run is not None, it will query the max queue id for the given run and dataset

        Returns:
            int: The max queue id from `i3filter.job`.
        """

        if run is None:
            query = self._dbs4.fetchall("SELECT MAX(queue_id) AS `max_queue_id` FROM i3filter.job WHERE dataset_id = {dataset_id}".format(dataset_id = dataset_id))
        else:
            query = self._dbs4.fetchall("SELECT MAX(queue_id) AS `max_queue_id` FROM i3filter.run WHERE dataset_id = {dataset_id} AND run_id = {run_id}".format(dataset_id = dataset_id, run_id = run.run_id))

        return query[0]['max_queue_id']

    def _submit_job(self, queue_id, dataset_id, run, sub_run_id, checksumcache, input_files):
        sql = """
                INSERT INTO i3filter.job (dataset_id, queue_id, status)
                VALUES ({dataset_id}, {queue_id},"{status}")
            """.format(dataset_id = dataset_id, queue_id = queue_id, status = 'WAITING')

        self.logger.debug('SQL: {0}'.format(sql))

        if not self.dryrun:
            self._dbs4.execute(sql)

        for f in input_files:
            sql = """
                    INSERT INTO i3filter.urlpath (dataset_id, queue_id, name, path, type, md5sum, size)
                    VALUES ({dataset_id}, {queue_id}, "{file_name}", "{file_path}", "INPUT", "{checksum}", {file_size})""".format(
                        dataset_id = dataset_id,
                        queue_id = queue_id,
                        file_name = f.get_name(),
                        file_path = self.path_prefix + f.get_dirname() + "/",
                        checksum = checksumcache.get_md5(f.path),
                        file_size = f.size())

            self.logger.debug('SQL: {0}'.format(sql))

            if not self.dryrun:
                self._dbs4.execute(sql)

        sql = """
            INSERT INTO i3filter.run (run_id, dataset_id, queue_id, sub_run, date)
            VALUES ({run_id}, {dataset_id}, {queue_id}, {sub_run}, "{date}")""".format(
                run_id = run.run_id,
                dataset_id = dataset_id,
                queue_id = queue_id,
                sub_run = sub_run_id,
                date = run.get_start_time().date_time.date())

        self.logger.debug('SQL: {0}'.format(sql))

        if not self.dryrun:
            self._dbs4.execute(sql)


    def submit_run(self, dataset_id, run, checksumcache, source_file_type, gcd_file = None, special_files = [], aggregate = 1, aggregate_only_first_files = 0, aggregate_only_last_files = 0):
        """
        Submits the run. It makes all the inserts to the database.

        Args:
            dataset_id (int): The dataset id
            run (runs.Run): The run
            checksumcache (files.ChecksumCache): The cache that manages the checksums
            source_file_type (str): `Level2`, `PFDST`, or `PFFilt`
            gcd_file (files.File|str): GCD file. If `None`, the GCD file will be looked up from the datawarehouse
            special_files (list): List of paths or files.File that are input file for *each* job of this run. If `None` or the list is empty, this option is ignored
            aggregate (int): Needs to be >= 1. If > 1, more than one input file will be processed by one job
            aggregate_only_last_files (int): In certain circumstances we need to aggregate the last files but not the other files. 0 or < 0 does nothing, if larger than 0, this is the number
                                             of files that will be *added* to the last job in addition. E.g. if aggregate_only_last_files = 2, the last job will have 3 input files.
        """

        self.logger.info(run.format('IceProd1: Submitting run {run_id}, snapshot_id = {snapshot_id}, production_version = {production_version}'))

        if aggregate <= 0 or aggregate > 500:
            self.logger.critical('Invalid value for `aggregate`: {0}'.format(aggregate))
            raise RuntimeError('Invalid value for `aggregate`')

        if aggregate > 1 and aggregate_only_last_files > 0:
            self.logger.critical('Invalid combination of parameters: you cannot enable `aggregate_only_last_files` and having `aggregate` > 1.')
            raise RuntimeError('Invalid combination of parameters: you cannot enable `aggregate_only_last_files` and having `aggregate` > 1.')

        if aggregate > 1 and aggregate_only_first_files > 0:
            self.logger.critical('Invalid combination of parameters: you cannot enable `aggregate_only_first_files` and having `aggregate` > 1.')
            raise RuntimeError('Invalid combination of parameters: you cannot enable `aggregate_only_first_files` and having `aggregate` > 1.')

        if aggregate > 1:
            self.logger.info('** You aggregate several files to one job: {0} **'.format(aggregate))

        self.logger.debug('Get {0} files'.format(source_file_type))
        if source_file_type == 'PFFilt':
            input_files = run.get_pffilt_files()
        elif source_file_type == 'PFDST':
            input_files = run.get_pfdst_files()
        elif source_file_type == 'Level2':
            input_files = run.get_level2_files()
        else:
            self.logger.critical('Unknown data source')
            raise RuntimeError('Unknown data source')

        # Sort files by sub run id
        input_files = SubRun.sort_sub_runs(input_files)

        if special_files is not None:
            special_files = [File(f, self.logger) for f in special_files]

            for f in special_files:
                if not f.exists():
                    self.logger.critical('Special file {0} does not exist'.format(f))
        else:
            special_files = []

        if gcd_file is None:
            self.logger.info('No GCD file has been specified. Find the correct on in datawarehouse')
            gcd_file = run.get_gcd_file()
        else:
            if not isinstance(gcd_file, File):
                gcd_file = File(gcd_file, self.logger)

        self.logger.debug("GCD file: {0}".format(gcd_file.path))

        if gcd_file is None:
            self.logger.critical("No GCD file found.")
            raise RuntimeError('No GCD file found')

        if not len(input_files):
            self.logger.critical('No {0} files have been found'.format(source_file_type))
            raise RuntimeError('No input files found')
        else:
            queue_id = self._get_max_queue_id(dataset_id)

            self.logger.debug('Last queue_id = {0}'.format(queue_id))

            self.logger.info("Attempting to submit {0} {2} files for run {1}".format(len(input_files), run.run_id, source_file_type))

            # Calculate number of jobs. Files/jobs is `aggregate`
            # If input_files % aggregate > 0, we need an additional job that processes the leftover files
            number_of_jobs = int(len(input_files) / aggregate) + int(bool(len(input_files) % aggregate))

            if aggregate_only_last_files > 0:
                # add aggregate_only_last_files jobs to the last job... therefore, we have aggregate_only_last_files less.
                number_of_jobs -= aggregate_only_last_files

            if aggregate_only_first_files > 0:
                # add aggregate_only_first_files jobs to the first job... therefore, we have aggregate_only_first_files less.
                number_of_jobs -= aggregate_only_first_files

            self.logger.debug('Number of jobs: {0}'.format(number_of_jobs))

            file_counter = 0
            for job_id in range(number_of_jobs):
                queue_id += 1

                self.logger.debug('Job #{0}'.format(job_id))

                job_input_files = [gcd_file]
                job_input_files.extend(special_files)

                for _ in range(aggregate):
                    self.logger.debug('Add file #{0}/{1} to job'.format(file_counter + 1, len(input_files)))

                    job_input_files.append(input_files[file_counter])
                    file_counter += 1

                    if len(input_files) <= file_counter:
                        self.logger.debug('Reached last input file')
                        break

                if aggregate_only_first_files > 0 and job_id == 0:
                    # Add short first files to the first job. File 0 has already been added...
                    self.logger.debug('Aggregate first {} files'.format(aggregate_only_first_files))

                    job_input_files.extend(input_files[file_counter : aggregate_only_first_files + 1])

                    # Next file will be...
                    file_counter = aggregate_only_first_files + 1

                if aggregate_only_last_files > 0 and job_id == number_of_jobs - 1:
                    # Add last file to the current job
                    self.logger.debug('Aggregate last {} files'.format(aggregate_only_last_files))

                    job_input_files.extend(input_files[file_counter :])

                self.logger.debug('Submit job #{1}, sub run ID {2}, with the following input files: {0}'.format(job_input_files, job_id, input_files[job_id].sub_run_id))

                self._submit_job(queue_id, dataset_id, run, input_files[job_id].sub_run_id, checksumcache, job_input_files)

    def clean_run(self, dataset_id, run):
        query  = self._dbs4.fetchall(run.format("""
            SELECT j.queue_id
            FROM i3filter.job j
            JOIN i3filter.run r
                ON j.queue_id = r.queue_id AND
                    j.dataset_id = r.dataset_id
            WHERE r.dataset_id = {dataset_id} AND
                r.run_id = {run_id}""", dataset_id = dataset_id))

        queue_ids = [str(q['queue_id']) for q in query]

        if not len(queue_ids):
            self.logger.info('Nothing to clean')
            return

        # Clean job table
        sql = "DELETE FROM i3filter.job WHERE dataset_id = {dataset_id} AND queue_id IN ({queue_ids})".format(
            dataset_id = dataset_id,
            queue_ids = ','.join(queue_ids)
        )

        self.logger.debug('Cleaning SQL: {0}'.format(sql))

        if not self.dryrun:
             self._dbs4.execute(sql)

        # Clean urlpath table
        sql = "DELETE FROM i3filter.urlpath WHERE dataset_id = {dataset_id} AND queue_id IN ({queue_ids})".format(
            dataset_id = dataset_id,
            queue_ids = ','.join(queue_ids)
        )

        self.logger.debug('Cleaning SQL: {0}'.format(sql))

        if not self.dryrun:
             self._dbs4.execute(sql)

        # Clean run table
        sql = "DELETE FROM i3filter.run WHERE dataset_id = {dataset_id} AND queue_id IN ({queue_ids})".format(
            dataset_id = dataset_id,
            queue_ids = ','.join(queue_ids)
        )

        self.logger.debug('Cleaning SQL: {0}'.format(sql))

        if not self.dryrun:
             self._dbs4.execute(sql)

    def get_run_status(self, dataset_id, run):
        sql = """
            SELECT COUNT(*) AS `jobs`,
                SUM(IF(j.status = "OK", 1, 0)) AS `ok`,
                SUM(IF(j.status = 'BadRun', 1, 0)) AS `bad_runs`,
                SUM(IF(j.status = 'ERROR', 1, 0)) AS `error`,
                SUM(IF(j.status = 'FAILED', 1, 0)) AS `failed`
            FROM i3filter.job j JOIN i3filter.run r ON j.queue_id = r.queue_id AND r.dataset_id = j.dataset_id
            WHERE j.dataset_id = {dataset_id} AND
                r.run_id = {run_id}""".format(dataset_id = dataset_id, run_id = run.run_id)

        self.logger.debug('SQL: {0}'.format(sql))

        query = self._dbs4.fetchall(sql)

        if not len(query) or query[0]['ok'] is None:
            return 'NOT_SUBMITTED'

        data = query[0]

        if data['jobs'] == data['ok'] + data['bad_runs']:
            return 'OK'
        elif data['error'] > 0 or data['failed'] > 0:
            return 'ERROR'
        else:
            return 'PROCESSING'

    def is_run_submitted(self, dataset_id, run):
        sql = """
            SELECT * FROM i3filter.run r
            JOIN i3filter.job j
                ON r.dataset_id = j.dataset_id
                    AND r.queue_id = j.queue_id
            WHERE r.dataset_id = {dataset_id:d}
                AND run_id = {run_id:d}""".format(dataset_id = dataset_id, run_id = run.run_id)

        self.logger.debug('SQL: {0}'.format(sql))

        query = self._dbs4.fetchall(sql)
        return len(query) > 0

    def get_jobs(self, dataset_id, run):
        sql = """
            SELECT j.job_id, u.path, u.name, u.md5sum, r.sub_run, u.type
            FROM i3filter.job j
            JOIN i3filter.urlpath u
                ON j.dataset_id = u.dataset_id AND
                    j.queue_id = u.queue_id
            JOIN i3filter.run r
                ON r.dataset_id = j.dataset_id AND
                    r.queue_id = j.queue_id
            WHERE j.dataset_id = {dataset_id} AND
                r.run_id = {run_id} AND
                u.type IN ('INPUT', 'PERMANENT')
        """.format(dataset_id = dataset_id, run_id = run.run_id)

        self.logger.debug('SQL: {0}'.format(sql))

        query = self._dbs4.fetchall(sql)
        result = {}

        for job in query:
            sub_run  = job['sub_run']

            if sub_run not in result:
                result[sub_run] = {'input': [], 'output': []}

            jresult = result[sub_run]
            jresult['job_id'] = job['job_id']

            # Input or output?
            filetype = None

            if job['type'] == 'INPUT':
                filetype = 'input'
            elif job['type'] == 'PERMANENT':
                filetype = 'output'
            else:
                raise RuntimeError('Type {0} is unexpected'.format(job['type']))

            jresult[filetype].append({
                'path': os.path.join(remove_path_prefix(job['path']), job['name']),
                'md5': job['md5sum']
            })

        return result

    def remove_file_from_catalog(self, dataset_id, run, path):
        """
        Removes file from file catalog. In the case of iceprod1 it just sets the transferstate to DELETED.

        Args:
            path (files.File): The file
        """

        sql = """
            UPDATE i3filter.urlpath u 
            JOIN i3filter.run r
                ON u.queue_id = r.queue_id AND
                    u.dataset_id = r.dataset_id
            SET u.transferstate = "DELETED"
            WHERE r.dataset_id = {dataset_id} AND
                r.run_id = {run_id} AND
                CONCAT(path, IF(RIGHT(path, 1) = '/', '', '/'), name) LIKE '%{path}'
        """.format(dataset_id = dataset_id, run_id = run.run_id, path = path.path)

        self.logger.debug('SQL: {0}'.format(sql))

        if not self.dryrun:
            self._dbs4.execute(sql)

    def update_file_in_catalog(self, dataset_id, run, path):
        """
        Args:
            path (files.File): The file
        """

        sql = """
            UPDATE i3filter.urlpath u
            JOIN i3filter.run r
                    ON u.queue_id = r.queue_id AND
                        u.dataset_id = r.dataset_id
            SET md5sum = '{md5}', size = {size}, transferstate = 'WAITING'
            WHERE r.dataset_id = {dataset_id} AND
                r.run_id = {run_id} AND
                CONCAT(path, IF(RIGHT(path, 1) = '/', '', '/'), name) LIKE '%{path}'
        """.format(dataset_id = dataset_id, run_id = run.run_id, path = path.path, md5 = path.md5(), size = path.size())

        self.logger.debug('SQL: {0}'.format(sql))
        if not self.dryrun:
            self._dbs4.execute(sql)

    def add_file_to_catalog(self, dataset_id, run, path):
        queue_id = self._get_max_queue_id(dataset_id, run = run)

        sql = """
            INSERT INTO i3filter.urlpath (dataset_id, queue_id, name, path, type, md5sum, size)
            VALUES ({dataset_id}, {queue_id}, '{name}', '{path}', 'PERMANENT', '{md5}', {size})
            ON DUPLICATE KEY UPDATE
                dataset_id = {dataset_id},
                queue_id = {queue_id},
                name = '{name}',
                path = '{path}',
                type = 'PERMANENT',
                md5sum = '{md5}',
                size = {size},
                transferstate = 'WAITING'
        """.format(
            dataset_id = dataset_id,
            queue_id = queue_id,
            name = path.get_name(),
            path = self.path_prefix + path.get_dirname(),
            md5 = path.md5(),
            size = path.size()
        )

        self.logger.debug('SQL: {0}'.format(sql))

        if not self.dryrun:
            self._dbs4.execute(sql)

