from boto.s3 import connection, key, prefix
import datetime
from io import StringIO
from IPython.html.services.notebooks.nbmanager import NotebookManager
from IPython.nbformat import current
from IPython.utils.traitlets import Unicode
import json
import os
from tornado import web
import uuid


class S3NotebookManager(NotebookManager):
    """
    An implementation of the NotebookManager for IPython using s3 as the backing store
    """

    aws_access_key_id = Unicode(config=True, help='AWS access key id.')
    aws_secret_access_key = Unicode(config=True, help='AWS secret access key.')
    s3_bucket = Unicode(u'', config=True, help='Bucket name for notebooks.')
    s3_prefix = Unicode(u'', config=True, help='Key prefix of notebook location')
    s3_host = Unicode(u's3.amazonaws.com', config=True, help='The AWS S3 endpoint host to connect to')
    notebook_dir = Unicode(u"", config=True)

    def __init__(self, **kwargs):
        super(S3NotebookManager, self).__init__(**kwargs)
        # Configuration of aws access keys default to '' since it's unicode.
        # boto will fail if empty strings are passed therefore convert to None
        access_key = self.aws_access_key_id if self.aws_access_key_id else None
        secret_key = self.aws_secret_access_key if self.aws_secret_access_key else None
        self.s3_con = connection.S3Connection(access_key, secret_key, host=self.s3_host)
        self.bucket = self.s3_con.get_bucket(self.s3_bucket)

        if not self.s3_prefix.endswith('/'):
            self.s3_prefix += '/'

    # The method create_checkpoint is called by the server when
    # the user selects "save and checkpoint". It must assign a
    # unique checkpoint id to the checkpoint. It is not clear what
    # the allowed values are, but strings work fine (they are not
    # shown to the user).
    def create_checkpoint(self, name, path=''):
        """Create a checkpoint of the current state of a notebook

        Returns a dictionary with entries "id" and
        "last_modified" describing the checkpoint.
        """
        assert name.endswith(self.filename_ext)
        assert self.notebook_exists(name, path)

        checkpoint_id = "checkpoint-%s" % uuid.uuid4()
        notebook = self.get_notebook(name, path)

        try:
            key = self.bucket.new_key(os.path.join(self.s3_prefix, path, '.ipynb_checkpoints', name, checkpoint_id))
            key.set_metadata('nbname', name)
            key.set_metadata('nbcheckpointid', checkpoint_id)

            notebook['created'] = notebook['created'].strftime('%Y-%m-%d %H:%M:%S')
            notebook['last_modified'] = notebook['last_modified'].strftime('%Y-%m-%d %H:%M:%S')

            key.set_contents_from_string(json.dumps(notebook))
        except Exception as e:
            raise web.HTTPError(400, u'Unexpected error while saving checkpoint: %s' % e)

        last_modified = notebook['last_modified']
        return dict(id=checkpoint_id, last_modified=last_modified)

    def create_notebook(self, model=None, path=''):
        """Create a new notebook and return its model with no content."""
        new_model = super(S3NotebookManager, self).create_notebook(model, path)

        try:
            nb_data = json.loads(new_model['ipynb'])
            notebook_name = nb_data['metadata']['name']
            s3_path = os.path.join(self.s3_prefix, path, notebook_name)

            new_model['name'] = notebook_name
            new_model['path'] = path
            new_model['last_modified'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

            key = self.bucket.new_key(s3_path)
            key.set_metadata('nbname', notebook_name)
            key.set_contents_from_string(json.dumps(new_model))
        except Exception as e:
            raise web.HTTPError(400, u'Unexpected error while saving notebook: %s' % e)

        self.log.debug("create_notebook(%s, '%s') -> %s", str(model), path, str(new_model))
        return new_model

    # There is a call to delete_checkpoint in the notebook handler
    # code, but there doesn't seem to be a way to actually delete a
    # checkpoint through the notebook interface, so the code
    # below is untested.
    def delete_checkpoint(self, checkpoint_id, name, path=''):
        """delete a checkpoint for a notebook"""
        self.log.debug("delete_checkpoints(%s,'%s', '%s')", repr(checkpoint_id), name, path)
        assert name.endswith(self.filename_ext)
        assert self.notebook_exists(name, path)

        for checkpoint in self.list_checkpoints(name, path):
            if checkpoint['name'] == checkpoint_id:
                self.bucket.delete_key(os.path.join(self.s3_prefix, path, '.ipynb_checkpoints',
                                                    name, checkpoint['name']))
                break

    def delete_notebook(self, name, path=''):
        """Delete a notebook"""
        self.log.debug("delete_notebook('%s', '%s')", str(name), str(path))
        assert name.endswith(self.filename_ext)
        assert self.notebook_exists(name, path)

        # Delete the checkpoints
        checkpoints = self.list_checkpoints(name, path)
        checkpoint_keys = [os.path.join(self.s3_prefix, '.ipynb_checkpoints', name, f['name'])
                           for f in checkpoints]
        self.bucket.delete_keys(checkpoint_keys)

        # Delete the notebook
        self.bucket.delete_key(os.path.join(self.s3_prefix, path, name))

    # The NotebookManager API says this method should be implemented,
    # but it doesn't seem to be called from anywhere except the above
    # method list_dirs.
    def get_dir_model(self, name, path=''):
        """Get the directory model given a directory name and its API style path.

        The keys in the model should be:
        * name
        * path
        * last_modified
        * created
        * type='directory'
        """
        # Create the directory model.
        model = dict()
        model['name'] = name
        model['path'] = path
        model['last_modified'] = datetime.datetime.utcnow()
        model['created'] = datetime.datetime.utcnow()
        model['type'] = 'directory'

        self.log.debug("get_dir_model('%s', '%s') -> %s", name, path, str(model))
        return model

    # The method get_notebook is called by the server
    # retrieve the contents of a notebook for rendering.
    def get_notebook(self, name, path='', content=True):
        """ Takes a path and name for a notebook and returns its model

        Parameters
        ----------
        name : str
            the name of the notebook
        path : str
            the URL path that describes the relative path for
            the notebook

        Returns
        -------
        model : dict
            the notebook model. If contents=True, returns the 'contents'
            dict in the model as well.
        """
        self.log.debug("get_notebook('%s', '%s', %s)", name, path, str(content))
        assert name.endswith(self.filename_ext)

        if not self.notebook_exists(name, path):
            raise web.HTTPError(404, u'Notebook does not exist: %s' % name)

        try:
            key = self.bucket.get_key(os.path.join(self.s3_prefix, path, name))
            notebook = json.loads(key.get_contents_as_string())
        except:
            raise web.HTTPError(500, u'Notebook cannot be read.')

        model = dict()
        model['type'] = 'notebook'
        model['name'] = name
        model['path'] = path
        model['created'] = datetime.datetime.strptime(notebook['created'], '%Y-%m-%d %H:%M:%S')
        model['last_modified'] = datetime.datetime.strptime(notebook['ipynb_last_modified'], '%Y-%m-%d %H:%M:%S')

        if content:
            try:
                with StringIO(notebook['ipynb']) as f:
                    nb = current.read(f, u'json')
                self.mark_trusted_cells(nb, path, name)
                model['content'] = nb
            except:
                raise web.HTTPError(500, u'Unreadable JSON notebook.')

        return model

    def info_string(self):
        return "Serving notebooks from s3. bucket name: %s" % self.s3_bucket

    # The method is_hidden is called by the server to check if
    # a path is hidden. In the file-based manager, this corresponds
    # to directories whose name starts with a dot.
    def is_hidden(self, path):
        """Does the API style path correspond to a hidden directory or file?

        Parameters
        ----------
        path : string
            The path to check. This is an API path (`/` separated,
            relative to base notebook-dir).

        Returns
        -------
        hidden : bool
            Whether the path is hidden.

        """
        if '.ipynb_checkpoints' in path:
            return True
        else:
            return False

    # The method list_checkpoints is called by the server to
    # prepare the list of checkpoints shown in the File menu
    # of the notebook. It returns a list of dictionaries, which
    # have the same structure as those returned by create_checkpoint.
    def list_checkpoints(self, name, path=''):
        """Return a list of checkpoints for a given notebook"""
        assert name.endswith(self.filename_ext)
        assert self.notebook_exists(name, path)

        checkpoints = []
        for entry in self.bucket.list(prefix=os.path.join(self.s3_prefix, path, '.ipynb_checkpoints', name, '/'),
                                      delimiter='/'):
            checkpoint = entry.get_contents_as_string()
            checkpoint_json = current.reads(checkpoint, u'json')
            checkpoints.append(dict(id=entry['checkpoint_id'],
                                    last_modified=checkpoint_json['last_modified']))
        checkpoint_info = sorted(checkpoints, key=lambda item: item['last_modified'])

        self.log.debug("list_checkpoints('%s', '%s') -> %s", name, path, str(checkpoint_info))
        return checkpoint_info

    # The method list_dirs is called by the server to identify
    # the subdirectories in a given path.
    def list_dirs(self, path):
        """List the directory models for a given API style path."""
        s3_path = os.path.join(self.s3_prefix, path)
        self.log.debug("list_dirs('%s')", s3_path)

        dirs = self.bucket.list(prefix=s3_path, delimiter='/')
        dirlist = []
        for entry in dirs:
            if type(entry) is prefix.Prefix and '.ipynb_checkpoints' not in entry.name:
                try:
                    # For some reason path is always empty
                    model = self.get_dir_model(entry.name.replace(self.s3_prefix, ''),
                                               entry.name.replace(self.s3_prefix, ''))
                    dirlist.append(model)
                except IOError:
                    pass

        dirlist = sorted(dirlist, key=lambda item: item['name'])

        self.log.debug("list_dirs -> %s", str(dirlist))
        return dirlist

    # The method list_notebooks is called by the server to prepare
    # the list of notebooks for a path given in the URL.
    # It is not clear if the existence of the path is guaranteed.
    def list_notebooks(self, path=''):
        """Return a list of notebook dicts without content.

        This returns a list of dicts, each of the form::

            dict(notebook_id=notebook,name=name)

        This list of dicts should be sorted by name::

            data = sorted(data, key=lambda item: item['name'])
        """
        s3_path = os.path.join(self.s3_prefix, path)

        notebooks = []
        for entry in self.bucket.list(prefix=s3_path, delimiter='/'):
            if type(entry) is key.Key:
                nb_name = entry.key[len(self.s3_prefix):]
                notebooks.append({'name': nb_name, 'notebook_id': nb_name})
        notebooks = sorted(notebooks, key=lambda item: item['name'])

        self.log.debug("list_notebooks('%s') -> %s", path, notebooks)
        return notebooks

    # The method notebook_exists is called by the server to verify the
    # existence of a notebook before rendering it.
    def notebook_exists(self, name, path=''):
        """Returns a True if the notebook exists. Else, returns False.

        Parameters
        ----------
        name : string
            The name of the notebook you are checking.
        path : string
            The relative path to the notebook (with '/' as separator)

        Returns
        -------
        bool
        """
        assert name.endswith(self.filename_ext)
        exists = self.path_exists(path) and name in [f['name'] for f in self.list_notebooks(path)]
        self.log.debug("notebook_exists('%s', '%s') -> %s", name, path, str(exists))
        return exists

    # The method path_exists is called by the server to check
    # if the path given in a URL corresponds to a directory
    # potentially containing notebooks, or to something else.
    def path_exists(self, path):
        """Does the API-style path (directory) actually exist?

        Parameters
        ----------
        path : string
            The path to check. This is an API path (`/` separated,
            relative to base notebook-dir).

        Returns
        -------
        exists : bool
            Whether the path is indeed a directory.

        Note: The empty path ('') must exist for the server to
              start up properly.
        """
        if path == '':
            return True
        else:
            if not path.endswith('/'):
                path += '/'

            all_dirs = self.list_dirs('')
            exists = path in [p['path'] for p in all_dirs]
            self.log.debug("path_exists('%s') -> %s", path, str(exists))
            return exists

    # The method restore_checkpoint is called by the server when
    # the user asks to restore the notebook state from a checkpoint.
    # The checkpoint is identified by its id, the notebook as
    # usual by name and path.
    def restore_checkpoint(self, checkpoint_id, name, path=''):
        """Restore a notebook from one of its checkpoints"""
        self.log.debug("restore_checkpoints(%s,'%s', '%s')", repr(checkpoint_id), name, path)
        assert name.endswith(self.filename_ext)
        assert self.notebook_exists(name, path)

        notebook = self.get_notebook(name, path, content=True)
        for entry in self.bucket.list(os.path.join(self.s3_prefix, path, '.ipynb_checkpoints', name), '/'):
            if entry['checkpoint_id'] == checkpoint_id:
                checkpoint = entry.get_contents_as_string()
                checkpoint_json = current.reads(checkpoint, u'json')
                notebook['ipynb_last_modified'] = checkpoint_json['ipynb_last_modified']
                notebook['ipynb'] = checkpoint_json
                self.save_notebook(notebook, name, path)
                break

    # The method save_notebook is called periodically
    # by the auto-save functionality of the notebook server.
    # It gets a model, which contains a name and a path,
    # plus explicit name and path arguments. When the user
    # renames a notebook, the new name and path are stored
    # in the model, and the next save operation causes a
    # rename of the file.
    # The code below also ensures that there is always a
    # checkpoint available, even before the first user-generated
    # checkpoint. It does so because FileNotebookManager does
    # the same. It is not clear if anything in the notebook
    # server requires this.
    def save_notebook(self, model, name, path=''):
        """Save the notebook model and return the model with no content."""

        self.log.debug("save_notebook(%s, '%s', '%s')", model, str(name), str(path))
        assert name.endswith(self.filename_ext)
        path = path.strip('/')

        if 'content' not in model:
            raise web.HTTPError(400, u'No notebook JSON data provided')

        # One checkpoint should always exist
        if self.notebook_exists(name, path) and not self.list_checkpoints(name, path):
            self.create_checkpoint(name, path)

        new_path = model.get('path', path)
        new_name = model.get('name', name)

        if path != new_path or name != new_name:
            self._rename_notebook(name, path, new_name, new_path)

        rightnow = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        notebook = dict(created=rightnow, checkpoints=[], name=new_name, path=new_path, last_modified=rightnow)

        # Save the notebook file
        nb = current.to_notebook_json(model['content'])
        nb['metadata']['name'] = unicode(new_name)
        self.check_and_sign(nb, new_path, new_name)
        nb['metadata']['name'] = unicode(new_name)
        #if 'name' in nb['metadata']:
        #    nb['metadata']['name'] = u''
        ipynb_stream = StringIO()
        current.write(nb, ipynb_stream, u'json')
        notebook['ipynb'] = ipynb_stream.getvalue()
        notebook['ipynb_last_modified'] = rightnow
        ipynb_stream.close()

        # Save .py script as well
        py_stream = StringIO()
        current.write(nb, py_stream, u'json')
        notebook['py'] = py_stream.getvalue()
        notebook['py_last_modified'] = rightnow
        py_stream.close()

        key = self.bucket.new_key(os.path.join(self.s3_prefix, new_name))
        key.set_metadata('nbname', new_name)
        key.set_contents_from_string(json.dumps(notebook))

        return notebook

    # The method update_notebook is called by the server
    # when the user renames a notebook. It is not quite clear
    # what the difference to saving under a new name it.
    def update_notebook(self, model, name, path=''):
        """Update the notebook's path and/or name"""
        self.log.debug("upate_notebook(%s, '%s', '%s')", str(model), name, path)
        assert name.endswith(self.filename_ext)
        assert self.notebook_exists(name, path)
        path = path.strip('/')

        new_name = model.get('name', name)
        new_path = model.get('path', path)
        if path != new_path or name != new_name:
            self._rename_notebook(name, path, new_name, new_path)
        model = self.get_notebook(new_name, new_path, content=False)
        self.log.debug("upate_notebook -> %s", str(model))
        return model

    #
    # Helper methods that are not part of the NotebookManager API
    #
    def _rename_notebook(self, name, path, new_name, new_path):
        """Rename a notebook."""
        assert name.endswith(self.filename_ext)
        assert new_name.endswith(self.filename_ext)
        assert self.notebook_exists(name, path)

        # Rename the checkpoints
        for entry in self.bucket.list(prefix=os.path.join(self.s3_prefix, path, '.ipynb_checkpoints', name, '/'),
                                      delimiter='/'):
            checkpoint = entry.get_contents_as_string()
            old_checkpoint = os.path.join(self.s3_prefix, path, '.ipynb_checkpoints', name,
                                          checkpoint['checkpoint_id'])
            new_checkpoint = os.path.join(self.s3_prefix, new_path, '.ipynb_checkpoints', new_name,
                                          checkpoint['checkpoint_id'])
            self.bucket.copy_key(new_checkpoint, self.s3_bucket, old_checkpoint)
            self.bucket.delete_key(old_checkpoint)

        # Rename the notebook
        new_notebook = os.path.join(self.s3_prefix, new_path, new_name)
        old_notebook = os.path.join(self.s3_prefix, path, name)
        self.bucket.copy_key(new_notebook, self.s3_bucket, old_notebook)
        self.bucket.delete_key(old_notebook)
