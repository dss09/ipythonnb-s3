# IPython notebook S3 store

[IPython notebook](http://ipython.org/ipython-doc/dev/interactive/htmlnotebook.html) manager which saves to [S3](http://aws.amazon.com/s3/).

Works with IPython 2.x

By default the notebook server stores the notebook files in a local directory.
This plugin seamlessly stores and reads the notebooks files from and to an S3 bucket.
It uses the great [boto](https://github.com/boto/boto) package to communicate with AWS.

It requires credentials to an AWS account.

## How to use

1. Install the package:
   
    ```
    $ git clone https://github.com/davidbrai/ipythonnb-s3.git
    $ cd ipythonnb-s3
    $ [sudo] python setup.py install
    ```
   
2. Create a profile for your notebook server by running:

    ```
    ipython profile create nbserver
    ```

3. Edit your `ipython_notebook_config.py` file (should be in ~/.ipython/profile_nbserver):

    ```python
    c.NotebookApp.notebook_manager_class = 's3nbmanager.S3NotebookManager'
    c.S3NotebookManager.aws_access_key_id = '<put your aws access key id here>'
    c.S3NotebookManager.aws_secret_access_key = '<put your aws secret access key here>'
    c.S3NotebookManager.s3_bucket = '<put the name of the bucket you want to use>'
    c.S3NotebookManager.s3_prefix = '<put the prefix of the key where your notebooks are stored>'
    ```

    If ``aws_access_key_id`` and ``aws_secret_access_key`` are not supplied
    then boto will look for the AWS keys in environment variables.

    If you store your notebooks in ``s3://bucket/simulations/notebooks/`` then
    set ``c.S3NotebookManager.s3_prefix = 'simulations/notebooks/'``

    You can also set ``c.S3NotebookManager.s3_host`` if you are using a region other than us-east-1

    The following options are useful if running the server as a background process

    ```python
    c.Application.verbose_crash = True
    c.NotebookApp.open_browser = False
    c.NotebookApp.port = 8888
    c.NotebookApp.ip = '*'
    c.NotebookApp.profile = 'nbserver'
    ```

4. Start notebook server:

    ```
    ipython notebook --profile=nbserver
    ```
