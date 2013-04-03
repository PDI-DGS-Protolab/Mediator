
import helpers
import os


class RiakCSConnector():

    def __init__(self):
        for op in ['create', 'get', 'list', 'list_all', 'remove']:
            name = op + "_bucket"
            setattr(RiakCSConnector, name, self.make_bucket(name))

        for op in ['get', 'upload', 'delete']:
            name = op + "_file"
            setattr(RiakCSConnector, name, self.make_file(name))

    def call(self, command):
        os.system(command)

    def get_operation(self, ops, operation):
        # TODO Handle this exception properly
        if not operation or not ops:
            raise Exception("Operation not supported")

        return ops[operation]

    def make_bucket(self, operation):

        def bucket_method(self, bucket_name=""):

            # Dictionary to translate method name into required command by the s3cmd tool
            ops = {"create_bucket": "mb",
                   "get_bucket": "ls",
                   "list_bucket": "ls",
                   "list_all_bucket": "la",
                   "remove_bucket": "rb"
                   }

            op = self.get_operation(ops, operation)

            if not bucket_name:
                bucket_name = helpers.timebox()

            if (operation == "list_all_bucket") or (operation == "get_bucket" and not bucket_name):
                command = "s3cmd " + op
            else:
                command = "s3cmd " + op + " s3://" + bucket_name

            self.call(command)

        return bucket_method

    def make_file(self, operation):

        def file_method(self, file_name, bucket_name, local_file=""):

            # Dictionary to translate method name into required command by the s3cmd tool
            ops = {"upload_file": "put",
                   "delete_file": "del",
                   "get_file": "get"
                   }

            op = self.get_operation(ops, operation)

            command = ""
            if operation == "upload_file":
                self.create_bucket(bucket_name)
                command = "s3cmd " + op + " " + file_name + " s3://" + bucket_name

            elif operation == "get_file" and not local_file:
                local_file = "riak_cs_" + file_name + "_" + helpers.now()
                command = "s3cmd " + op + " s3://" + bucket_name + "/" + file_name + " " + local_file

            else:
                command = "s3cmd " + op + " s3://" + bucket_name + "/" + file_name + " " + local_file

            self.call(command)

        return file_method

if __name__ == "__main__":
    cs = RiakCSConnector()
    """
    bucket = "bucket.testing.2013.04.03"
    cs.create_bucket(bucket)
    cs.upload_file("salva", bucket)
    cs.list_all_bucket()
    cs.list_bucket("bucket.testing.oil.upm")
    cs.get_bucket()
    """
