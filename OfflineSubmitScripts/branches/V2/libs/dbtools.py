
def get_bad_sub_runs(dbs4, dataset_id, run_id, logger):
    sql = """SELECT 
                sub_run,
                path,
                name
            FROM
                i3filter.job j
                    JOIN
                i3filter.run r ON r.queue_id = j.queue_id
                    AND r.dataset_id = j.dataset_id
                    JOIN
                i3filter.urlpath u ON u.queue_id = j.queue_id
                    AND u.dataset_id = j.dataset_id
            WHERE
                j.dataset_id = %s AND run_id = %s
                    and status = 'BadRun';""" % (dataset_id, run_id)

    result = dbs4.fetchall(sql, UseDict = True)

    return result
