
def max_queue_id(dbs4_, DatasetId):
    """
    Retrieves the max queue id from the `i3filter.job` table.

    Args:
        dbs4_ (SQLClient_dbs4): The SQL client for dbs4.
        DatasetId (int): The dataset id

    Returns:
        int: The max queue id from `i3filter.job`.
    """
    # Get current maximum queue_id for dataset_id, any subsquent submissions starts from queue_id+1
    tmp = dbs4_.fetchall("""select max(queue_id) from i3filter.job where dataset_id=%s """%(DatasetId))
    return tmp[0][0]

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
