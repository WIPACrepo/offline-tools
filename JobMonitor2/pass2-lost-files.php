<?php

require('config.php');
require('resources/class.TestRuns.php');

header('Content-Type: application/json');

$db = new mysqli($CONFIG['filter_db_host'], $CONFIG['filter_db_username'], $CONFIG['filter_db_password'], $CONFIG['filter_db_database']);

$tr = new TestRuns($db);

$sql = "SELECT * FROM i3filter.missing_files_pass2 ORDER BY run_id, sub_run, type";
$query = $db->query($sql);

$data = array();
while($row = $query->fetch_assoc()) {
    $data[] = $row + array('season' => $tr->get_season_by_run_id($row['run_id']));
}

if(isset($_GET['datatables']) && strtolower($_GET['datatables']) == 'true') {
    print(json_encode(array('data' => $data)));
} else {
    print(json_encode($data));
}
