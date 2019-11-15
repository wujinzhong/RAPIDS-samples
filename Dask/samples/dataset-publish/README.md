Steps:
1.	Command line start a Dask scheduler
    >>>dask-scheduler --port 8487 &
    Scheduler at:   tcp://127.0.0.1:8487
2.	Command line create a worker
    >>>dask-cuda-worker 127.0.0.1:8487 &
    distributed.worker - INFO -       Start worker at:   tcp://127.0.0.1:32869
    distributed.worker - INFO -       Start worker at:   tcp://127.0.0.1:36949
    distributed.worker - INFO -       Start worker at:   tcp://127.0.0.1:43955
    distributed.worker - INFO -       Start worker at:   tcp://127.0.0.1:43935
    distributed.worker - INFO -       Start worker at:   tcp://127.0.0.1:43273
    distributed.worker - INFO -       Start worker at:   tcp://127.0.0.1:38341
    distributed.worker - INFO -       Start worker at:   tcp://127.0.0.1:40173
    distributed.worker - INFO -       Start worker at:   tcp://127.0.0.1:46397
3.	python ./publish_datasets.py
4.  python ./use_publish_datasets.py
