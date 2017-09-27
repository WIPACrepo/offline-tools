<?php

class Timer {
    private $start;
    private $stop;

    public function start() {
        $this->start = microtime(true);
        return $this;
    }

    public function stop() {
        $this->stop = microtime(true);
        return $this;
    }

    public function elapsed() {
        return $this->stop - $this->start;
    }

    public function print_elapsed($title = null) {
        if(is_null($title)) {
            print("Elapsed time: {$this->elapsed()}s\n");
        } else {
            print("[{$title}] Elapsed time: {$this->elapsed()}s\n");
        }

        return $this;
    }
}
