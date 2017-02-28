<?php

class Tools {
    private static $config;

    public static function set_config($config) {
        self::$config = $config;
    }

    public static function remove_path_prefix($path) {
        // Since the path starts with a 'file:' or 'gsiftp://gridftp.icecube.wisc.edu', remove it
        foreach(self::$config['path_prefixes'] as $prefix) {
            $len = strlen($prefix);

            if(substr($path, 0, $len) == $prefix) {
                return substr($path, $len);
            }
        }

        return $path;
    }

    public static function join_paths() {
        // Found here: http://stackoverflow.com/a/15575293
        $paths = array();
    
        foreach (func_get_args() as $arg) {
            if ($arg !== '') { $paths[] = $arg; }
        }
    
        return preg_replace('#/+#','/',join('/', $paths));
    }
}
