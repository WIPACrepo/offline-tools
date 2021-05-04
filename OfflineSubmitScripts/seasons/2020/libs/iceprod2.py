
import os
from . import iceprodinterface
import getpass,base64
import pathlib
from .files import File
from libs.databaseconnection import DatabaseConnection
from .path import remove_path_prefix
from .runs import SubRun
from .iceprod_tools.rest3 import IPRest
import json

class IceProd2(iceprodinterface.IceProdInterface):
    def __init__(self, logger, dryrun, username, authtok):
        super(IceProd2, self).__init__(logger, dryrun)

        self._db = DatabaseConnection.get_connection('filter-db', logger)
        self.path_prefix = 'gsiftp://gridftp.icecube.wisc.edu'
        self.increment = 100


        rro = IPRest(url="https://iceprod2-api.icecube.wisc.edu/", auth = 'Bearer %s'% authtok)
        if username:
             tok = rro.auth2(username,getpass.getpass())
             sessiontok = tok['token']
        else:
             sessiontok = authtok
        self.iprest = IPRest(url="https://iceprod2-api.icecube.wisc.edu/", auth = 'Bearer %s'% sessiontok)




    def _get_max_queue_id(self, dataset, run = None):
        """
        Retrieves the max queue id from the `i3filter.job` table.

        Args:
            dataset_id (int): The dataset id
            run (runs.Run): If run is not None, it will query the max queue id for the given run and dataset

        Returns:
            int: The max queue id from `i3filter.job`.
        """
        dataset_id = dataset['dataset_id']
        self.logger.debug("_get_max_queue_id for dataset %s" % dataset_id)
        if run is None:
            query = self._db.fetchall("SELECT MAX(queue_id) AS `max_queue_id` FROM i3filter.dataset_subruns WHERE dataset_id = {dataset_id}".format(dataset_id = dataset_id))
        else:
            query = self._db.fetchall("SELECT MAX(queue_id) AS `max_queue_id` FROM i3filter.dataset_subruns WHERE dataset_id = {dataset_id} AND run_id = {run_id}".format(dataset_id = dataset_id, run_id = run.run_id))
        self.logger.debug(query)
        if query[0]['max_queue_id'] is None: return -1

        return query[0]['max_queue_id']


    def _get_current_subruns(self, dataset, run):
        subruns = {}
        sql = """ SELECT * FROM i3filter.dataset_subruns WHERE 
            dataset_id = {dataset_id} AND run_id = {run_id} 
            """.format(dataset_id = dataset['dataset_id'], run_id = run.run_id)
        self.logger.debug(sql)
        query  = self._db.fetchall(sql) 
        for entry in query:
            subruns[entry['sub_run']] = entry
            self.logger.debug(entry)
        return subruns

 

    def _submit_job(self, queue_id, dataset, run, sub_run_id, task, checksumcache, input_files, dataset_config):
        self.logger.debug("_submit_job")
        dataset_id = dataset['dataset_id']
        task_id = task['task_id']

   
        sql = """
                INSERT INTO i3filter.dataset_subruns (dataset_id, queue_id, run_id, sub_run, task_id, status, date)
                VALUES ({dataset_id}, {queue_id}, {run_id}, {sub_run}, "{task_id}", "{status}", "{date}")
            """.format(dataset_id = dataset_id, queue_id = queue_id, 
            run_id = run.run_id, sub_run = sub_run_id, task_id = task_id, status = 'waiting', 
            date = run.get_start_time().date_time.date())

        self.logger.debug('SQL: {0}'.format(sql))

        if not self.dryrun:
            self._db.execute(sql)

        ifile = 0


        for f in input_files:
            file_name = f.get_name()
            file_path = self.path_prefix + f.get_dirname() + "/"
            checksum = checksumcache.get_md5(f.path)
            file_size = f.size()

            local_name = file_name
            suffixes = pathlib.Path(file_name).suffixes
            suffix = suffixes[-1]
            if f.file_type == 'gcd':
                local_name = 'gcdfile{0}'.format("".join(suffixes[-2:]))
            elif f.file_type == 'subrun':
                local_name = 'infile{0:0>8d}{1}'.format(ifile,"".join(suffixes[-2:]))

            file_dict = { 
                    "filename":file_path+file_name,
                    "movement":"input",
                    "type":"permanent",
                    "compression":False,
                    "job_index":queue_id,
                    "task_name":"filtering",
                    "local":local_name
                    }


            self.logger.debug('infile: {0}->{1}'.format(file_dict['filename'],file_dict['local']))
            if not self.dryrun:
                   cfg = self.iprest.add_files(dataset['iceprod_id'], file_dict, task_id=task_id, md5sum=False)

            if f.file_type == 'subrun' and ifile == 0:
                outfiles = dataset_config['steering']['parameters']['FILTER::outfiles']
                outfile_dir = dataset_config['steering']['parameters']['FILTER::outfile_dir']
                outfile_dir = run.format(outfile_dir,subrun=sub_run_id) 
                outfile_dict = { 
                        "filename":None,
                        "movement":"output",
                        "type":"permanent",
                        "compression":False,
                        "job_index":queue_id,
                        "task_name":"filtering",
                        "local":None
                        }


                for o in outfiles:
                    out_remote = run.format(o['remote'],subrun=sub_run_id,index=ifile)
                    out_local  = run.format(o['local'],subrun=sub_run_id,index=ifile)
                    outfile_dict['filename'] = self.path_prefix + outfile_dir + out_remote
                    outfile_dict['local'] = out_local
                    self.logger.debug('outfile: {0}'.format(outfile_dict['filename']))
                    if not self.dryrun:
                        cfg = self.iprest.add_files(dataset['iceprod_id'], outfile_dict, 
                                task_id=task_id,  md5sum=True)
            if f.file_type == 'subrun':
                ifile += 1

        if not self.dryrun:
            self.iprest.set_status(dataset['iceprod_id'],'waiting',task_id=task_id)


    def submit_run(self, dataset, run, checksumcache, source_file_type, gcd_file = None, special_files = [], aggregate = 1, aggregate_only_first_files = 0, aggregate_only_last_files = 0):
        """
        Submits the run. It makes all the inserts to the database.

        Args:
            dataset (dict): The dataset 
            run (runs.Run): The run
            checksumcache (files.ChecksumCache): The cache that manages the checksums
            source_file_type (str|tuple): `Level2`, `Level2pass2`, `PFDST`, `PFFilt`, or `('LevelX', <dataset_id>)`
            gcd_file (files.File|str): GCD file. If `None`, the GCD file will be looked up from the datawarehouse
            special_files (list): List of paths or files.File that are input file for *each* job of this run. If `None` or the list is empty, this option is ignored
            aggregate (int): Needs to be >= 1. If > 1, more than one input file will be processed by one job
            aggregate_only_last_files (int): In certain circumstances we need to aggregate the last files but not the other files. 0 or < 0 does nothing, if larger than 0, this is the number
                                             of files that will be *added* to the last job in addition. E.g. if aggregate_only_last_files = 2, the last job will have 3 input files.
        """

        self.logger.info(run.format('IceProd2: Submitting run {run_id}, snapshot_id = {snapshot_id}, production_version = {production_version}'))
        dataset_id = dataset['dataset_id']

        subrun_dict = self._get_current_subruns(dataset, run)

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
        elif source_file_type == 'Level2pass2':
            input_files = run.get_level2pass2_files()
        elif isinstance(source_file_type, tuple) and len(source_file_type) == 2 and source_file_type[0] == 'LevelX':
            input_files = run.get_levelx_files(source_file_type[1])
        else:
            self.logger.critical('Unknown data source')
            raise RuntimeError('Unknown data source')

        # Sort files by sub run id
        input_files = SubRun.sort_sub_runs(input_files)
        self.logger.debug("subrun dict size: %s" % len(subrun_dict))
        self.logger.debug("input_files: %s" % len(input_files))
        input_files = list(filter(lambda f:f.sub_run_id not in subrun_dict,input_files))
        self.logger.debug("input_files after filter: %s" % len(input_files))

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

        if not gcd_file.exists():
            self.logger.critical('The GCD file does not exist: {}'.format(gcd_file))
            raise RuntimeError('The GCD file does not exist: {}'.format(gcd_file))

        self.logger.debug("GCD file: {0}".format(gcd_file.path))

        if gcd_file is None:
            self.logger.critical("No GCD file found.")
            raise RuntimeError('No GCD file found')

        if not len(input_files):
            self.logger.critical('No {0} files have been found'.format(source_file_type))
            return
        else:
            queue_id = self._get_max_queue_id(dataset) 
            dataset_info = self.iprest.get_dataset(dataset['iceprod_id'])
            dataset_config = self.iprest.get_config(dataset['iceprod_id'])
            self.logger.debug(json.dumps(dataset_info).encode('utf-8'))


            self.logger.info("Attempting to submit {0} {2} files for run {1}".format(len(input_files), run.run_id, source_file_type))

            # AGGEGRATE -- HOW IT WORKS
            # A requirement from the LE WG is that we make a mapping that is simple to understand. Only sub run ids should count, not the actual number of files.
            # This usually doesn't matter unless there is no missing file.
            # So, if we aggregate 10 files, the mapping should be
            #   sub runs 0-9 -> job 1
            #   sub runs 10-19 -> job 2
            #   ...
            # Therefore, in order to calculate the total job number, we assume that we have all sub runs.
            # Calculate number of jobs. Files/jobs is `aggregate`
            # If input_files[-1].sub_run_id % aggregate > 0, we need an additional job that processes the leftover files
            number_of_jobs = int((input_files[-1].sub_run_id + 1) / aggregate) + int(bool((input_files[-1].sub_run_id + 1) % aggregate))

            self.logger.debug('Last queue_id = {0}'.format(queue_id))
            if dataset_info['jobs_submitted'] < queue_id + number_of_jobs+1: 
                if not self.dryrun:
                    self.iprest.set_status(dataset['iceprod_id'],'processing')
                    self.iprest.set_jobs(dataset['iceprod_id'],queue_id+number_of_jobs+1)
                    self.iprest.buffer_jobs_tasks(dataset['iceprod_id'],num=number_of_jobs+1)

            cfg = self.iprest.tasks(dataset['iceprod_id'])
            max_job_index = -1
            tasks = {}
            for k,v in cfg.items():
                max_job_index = max(max_job_index,v['job_index'])
                tasks[v['job_index']] = v

            if max_job_index < queue_id + number_of_jobs:
                self.logger.error('task queue has not been populated yet. Current queue: {0}. Number of jobs:{1}'.format(queue_id+1,number_of_jobs))
                return


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

                if aggregate > 1:
                    self.logger.info('Job #{0}, queue_id #{1}'.format(job_id,queue_id))
                else:
                    self.logger.debug('Job #{0}, queue_id #{1}'.format(job_id,queue_id))

                # Add file type classifier
                gcd_file.file_type = 'gcd'
                for f in special_files:f.file_type ='special'
                for f in input_files:f.file_type ='subrun'

                job_input_files = [gcd_file]
                job_input_files.extend(special_files)

                input_files_added = False

                for _ in range(aggregate):
                    if (job_id * aggregate) <= input_files[file_counter].sub_run_id <= ((job_id + 1) * aggregate - 1):
                        if aggregate > 1:
                            self.logger.info('Add file #{0}/{1} (sub run #{2}/{3}) to job'.format(file_counter + 1, len(input_files), input_files[file_counter].sub_run_id, input_files[-1].sub_run_id))
                        else:
                            self.logger.debug('Add file #{0}/{1} (sub run #{2}/{3}) to job'.format(file_counter + 1, len(input_files), input_files[file_counter].sub_run_id, input_files[-1].sub_run_id))

                        job_input_files.append(input_files[file_counter])
                        file_counter += 1
                        input_files_added = True

                        if len(input_files) <= file_counter:
                            if aggregate > 1:
                                self.logger.info('Reached last input file')
                            else:
                                self.logger.debug('Reached last input file')
                            break
                    else:
                        if aggregate > 1:
                            self.logger.info('We have probably missing files. Therefore, this file has been added: {}'.format(input_files[file_counter]))
                        else:
                            self.logger.debug('We have probably missing files. Therefore, this file has been added: {}'.format(input_files[file_counter]))

                if aggregate_only_first_files > 0 and job_id == 0:
                    # Add short first files to the first job. File 0 has already been added...
                    self.logger.debug('Aggregate first {} files'.format(aggregate_only_first_files))

                    if aggregate_only_first_files > 10:
                        self.logger.warning('*** There are too many files to aggregate at the beginning of the run. '
                                +'We assume that it is sufficient to submit two files withing this job and ignore the others.**')
                        # Remove the first file that is added within the `for _ in range(aggregate)` loop.
                        del job_input_files[-1]
                        job_input_files.extend(input_files[aggregate_only_first_files - 2 : aggregate_only_first_files + 1])
                    else:
                        job_input_files.extend(input_files[file_counter : aggregate_only_first_files + 1])

                    # Next file will be...
                    file_counter = aggregate_only_first_files + 1

                if aggregate_only_last_files > 0 and job_id == number_of_jobs - 1:
                    # Add last file to the current job
                    self.logger.debug('Aggregate last {} files'.format(aggregate_only_last_files))

                    if aggregate_only_last_files > 10:
                        self.logger.warning('*** There are too many files to aggregate at the end of the run. '
                                +'We assume that it is sufficient to submit two files withing this job and ignore the others.**')
                        job_input_files.extend(input_files[file_counter : file_counter + 2])
                    else:
                        job_input_files.extend(input_files[file_counter :])


                if input_files_added:
                    self.logger.debug('Submit job #{1}, sub run ID {2}, with the following input files: {0}'.format(
                                job_input_files, job_id, input_files[job_id].sub_run_id))

                    self._submit_job(queue_id, dataset, run, input_files[job_id].sub_run_id, 
                            tasks[queue_id], checksumcache, job_input_files, dataset_config)
                else:
                    self.logger.warning('Job #{} has not been submitted since no input files have been added.'.format(job_id)
                           +' This could be caused by missing files.')

    def clean_run(self, dataset, run):
        self.logger.info("clean_run")
        dataset_id = dataset['dataset_id']
        query  = self._db.fetchall(run.format("""
            SELECT queue_id 
            FROM i3filter.dataset_subruns
            WHERE dataset_id = {dataset_id} AND
                run_id = {run_id}""", dataset_id = dataset_id))

        queue_ids = [str(q['queue_id']) for q in query]

        if not len(queue_ids):
            self.logger.info('Nothing to clean')
            return

        # Clean job table
        sql = "DELETE FROM i3filter.dataset_subruns WHERE dataset_id = {dataset_id} AND queue_id IN ({queue_ids})".format(
            dataset_id = dataset_id,
            queue_ids = ','.join(queue_ids)
        )

        self.logger.debug('Cleaning SQL: {0}'.format(sql))

        if not self.dryrun:
             self._db.execute(sql)

        # Clean urlpath table
        sql = "DELETE FROM i3filter.urlpath WHERE dataset_id = {dataset_id} AND queue_id IN ({queue_ids})".format(
            dataset_id = dataset_id,
            queue_ids = ','.join(queue_ids)
        )

        self.logger.debug('Cleaning SQL: {0}'.format(sql))

        #if not self.dryrun:
        #     self._db.execute(sql)

        # Clean run table
        sql = "DELETE FROM i3filter.dataset_subruns WHERE dataset_id = {dataset_id} AND queue_id IN ({queue_ids})".format(
            dataset_id = dataset_id,
            queue_ids = ','.join(queue_ids)
        )

        self.logger.debug('Cleaning SQL: {0}'.format(sql))

        if not self.dryrun:
             self._db.execute(sql)

    def get_run_status(self, dataset_id, run):
        self.logger.debug("get_run_status")

        #sql = """
        #       SELECT * from i3filter.datasets
        #       WHERE dataset_id = {dataset_id} 
        #     """.format(dataset_id = dataset_id)
        #self.logger.debug('SQL: {0}'.format(sql))
        #query = self._db.fetchall(sql)
        #dataset = query[0]

        #jobs = self.iprest.get_dataset_task_counts(dataset['iceprod_id'])
        #self.logger.debug('jobs: {0}'.format(jobs))
        #sql = """
        #    SELECT COUNT(*) AS `jobs`,
        #        SUM(IF(j.status = "OK", 1, 0)) AS `ok`,
        #        SUM(IF(j.status = 'BadRun', 1, 0)) AS `bad_runs`,
        #        SUM(IF(j.status = 'ERROR', 1, 0)) AS `error`,
        #        SUM(IF(j.status = 'FAILED', 1, 0)) AS `failed`
        #    FROM i3filter.dataset_subruns j 
        #    WHERE j.dataset_id = {dataset_id} AND
        #        j.run_id = {run_id}""".format(dataset_id = dataset_id, run_id = run.run_id)

        #self.logger.debug('SQL: {0}'.format(sql))
        #query = self._db.fetchall(sql)
        #total = sum(jobs.values())

        #self.logger.debug('jobs.values(): {0}'.format(jobs.values()))
        #self.logger.debug('jobs: {0}'.format(jobs))
        ##print(jobs)
        ##print(sum(jobs.values()))
        ##print(total)
        ##print(jobs.values())
        #self.logger.debug('sum(jobs.values()): {0}'.format(sum(jobs.values())))
        #self.logger.debug('query: {0}'.format(query))
        #self.logger.debug('query[0][ok]: {0}'.format(query[0]['ok']))
        #self.logger.debug('len(query): {0}'.format(len(query)))
        #self.logger.debug('total: {0}'.format(total))
        #self.logger.debug('jobs[complete]: {0}'.format(jobs['complete']))

        #'processing','failed','suspended','errors','complete'
    
        #if not total or jobs['complete'] is None:
        #if not len(query) or query[0]['ok'] is None:
        if not self.is_run_submitted(dataset_id, run):
            return 'NOT_SUBMITTED'

        #data = query[0]
        #if not data['bad_runs']:
        #   data['bad_runs'] = 0
        #if not 'complete' in jobs:
        #   jobs['complete'] = 0
        
        iceprod_id = self.get_iceprod_id(dataset_id)
        tasks = self.get_tasks(dataset_id, run)
        # Loop over task_ids to get each status
        for task in tasks:
            #self.logger.debug("task: {0}".format(task))
            #self.logger.debug("tasks[task]: {0}".format(tasks[task]))
            self.logger.debug("task: {0} tasks[task]: {1}".format(task,tasks[task]))
            iptask = self.iprest.tasks(iceprod_id, task_id = tasks[task])
            self.logger.debug("iptask: {0}".format(iptask))
            self.logger.debug("iptask['status']: {0}".format(iptask['status']))
            # Return first non-complete status
            if iptask['status'] != 'complete':
                return iptask['status']

        return 'OK'

        #if total == jobs['complete'] + data['bad_runs']:
        #    return 'OK'
        #elif 'complete' in jobs and jobs['complete'] > 0:
        ##elif jobs['errors'] > 0 or jobs['failed'] > 0:
        #    return 'OK'
        #else:
        #    return 'ERROR'

    def is_run_submitted(self, dataset_id, run):
        self.logger.debug("is_run_submitted")
        #return
        sql = """
            SELECT * FROM i3filter.dataset_subruns 
            WHERE dataset_id = {dataset_id:d}
                AND run_id = {run_id:d}""".format(dataset_id = dataset_id, run_id = run.run_id)

        self.logger.debug('SQL: {0}'.format(sql))

        query = self._db.fetchall(sql)
        return len(query) > 0

    def get_iceprod_id(self, dataset_id):
        self.logger.debug("get_iceprod_id")
        sql = """
               SELECT * from i3filter.datasets
               WHERE dataset_id = {dataset_id} 
             """.format(dataset_id = dataset_id)
        self.logger.debug('SQL: {0}'.format(sql))
        query = self._db.fetchall(sql)
        dataset = query[0]
        iceprod_id = dataset['iceprod_id']
        return iceprod_id

    def get_tasks(self, dataset_id, run):
        self.logger.debug("get_tasks")
        tasks = {}
        sql = """ SELECT * FROM i3filter.dataset_subruns WHERE 
            dataset_id = {dataset_id} AND run_id = {run_id} 
            ORDER BY sub_run
            """.format(dataset_id = dataset_id, run_id = run.run_id)
        self.logger.debug('SQL: {0}'.format(sql))
        query  = self._db.fetchall(sql) 
        #self.logger.debug('query: {0}'.format(query))
        iceprod_id = self.get_iceprod_id(dataset_id)
        self.logger.debug('iceprod_id: {0}'.format(iceprod_id))
        for entry in query:
            self.logger.debug('entry: {0}'.format(entry))
            tasks[entry['sub_run']] = entry['task_id']
            #iptask = self.iprest.tasks(iceprod_id, task_id = entry['task_id'])
            #self.logger.debug('iptask: {0}'.format(iptask))
        return tasks

    def get_logs(self, iceprod_id, task_id):
        #self.logger.debug("get_logs")
        self.logger.debug('iceprod_id: {0} task_id: {1}'.format(iceprod_id, task_id))
        logs = self.iprest.get_logs(iceprod_id, task_id)
        return logs

    def get_jobs(self, dataset_id, run):
        self.logger.debug("get_jobs")
        sql = """
               SELECT * from i3filter.datasets
               WHERE dataset_id = {dataset_id} 
             """.format(dataset_id = dataset_id)
        self.logger.debug('SQL: {0}'.format(sql))
        query = self._db.fetchall(sql)
        dataset = query[0]
        iceprod_id = dataset['iceprod_id']

        sql = """
            SELECT s.task_id, s.sub_run
            FROM i3filter.dataset_subruns s
            JOIN i3filter.runs r
                ON r.run_id = s.run_id
            WHERE s.dataset_id = {dataset_id} AND
                r.run_id = {run_id} 
        """.format(dataset_id = dataset_id, run_id = run.run_id)

        self.logger.debug('SQL: {0}'.format(sql))

        query = self._db.fetchall(sql)
        result = {}

        for job in query:
            sub_run = job['sub_run']
            task_id = job['task_id']

            if sub_run not in result:
                result[sub_run] = {'input': [], 'output': []}

            jresult = result[sub_run]
            jresult['job_id'] = task_id
            fs = self.iprest.get_files(iceprod_id,task_id)
            for f in fs['files']:

                if f['transfer'] == 'exists': continue
                # Input or output?
                filetype = f['movement']
                jresult[filetype].append({ 
                        'path': remove_path_prefix(f['remote']), 
                        'sha512': remove_path_prefix(f['remote'])+'.sha512'
                        })

        return result

    def remove_file_from_catalog(self, dataset_id, run, path):
        """
        Removes file from file catalog. In the case of iceprod1 it just sets the transferstate to DELETED.

        Args:
            path (files.File): The file
        """
        #self.logger.info("remove_file_from_catalog")
        self.logger.debug("remove_file_from_catalog")
        return

    def update_file_in_catalog(self, dataset_id, run, path):
        """
        Args:
            path (files.File): The file
        """

        self.logger.info("update_file_in_catalog")
        return

    def add_file_to_catalog(self, dataset_id, run, path):
        self.logger.info("add_file_to_catalog")
        return

