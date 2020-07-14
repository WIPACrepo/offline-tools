<?php

#require_once('class.Timer.php');


class Rest {

	private $rotok;

	public function __construct($base_url, $rotok){ 
		$this->rotok = $rotok;
		$this->base_url = $base_url;
	}

	public function httpPost($url, $data) 
	{ 
		$curl = curl_init($this->base_url.$url); 
		curl_setopt($curl, CURLOPT_HTTPHEADER, array("Authorization: Bearer {$this->rotok}") ); 
		curl_setopt($curl, CURLOPT_POST, true); 
		curl_setopt($curl, CURLOPT_POSTFIELDS, http_build_query($data)); 
		curl_setopt($curl, CURLOPT_RETURNTRANSFER, true); 
		$response = curl_exec($curl); 
		curl_close($curl); 
		return $response; 
	}


	public function httpGet($url, $data) 
	{ 
		$curl = curl_init($this->base_url.$url); 
		curl_setopt($curl, CURLOPT_HTTPHEADER, array("Authorization: Bearer {$this->rotok}") ); 
		curl_setopt($curl, CURLOPT_GET, true); 
		curl_setopt($curl, CURLOPT_GETFIELDS, http_build_query($data)); 
		curl_setopt($curl, CURLOPT_RETURNTRANSFER, true); 
		$response = curl_exec($curl); 
		curl_close($curl); 
		return $response; 
	}
}

