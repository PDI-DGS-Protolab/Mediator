
import helpers
import subprocess


class RiakCSConnector():

    def call(self, command):
        out = subprocess.check_output(command, shell=True)
        return out

    def filter_output(self, out):
        lines = out.split('\n')
        lines = lines[:-1]  # We take every line except the last '\n'

        buckets = []
        for l in lines:
            i = l.find('s3://')
            s = l[i:]
            buckets.append(s)

        print buckets
        return buckets

    def create_bucket(self, bucket_name):
        if not bucket_name:
                bucket_name = helpers.timebox()

        command = "s3cmd mb s3://" + bucket_name
        self.call(command)

    def list_buckets_names(self):
        out = self.call("s3cmd ls")
        return out

    def get_filenames_from_bucket(self, bucket_name):
        command = "s3cmd ls s3://" + bucket_name
        out = self.call(command)
        # out = self.filter_output(out)
        return out

    def get_filenames_from_buckets(self):
        command = "s3cmd la"
        out = self.call(command)
        # out = self.filter_output(out)
        return out

    def remove_bucket(self, bucket_name):
        command = "s3cmd rb s3://" + bucket_name
        self.call(command)

    def upload_file(self, file_name, bucket_name):
        self.create_bucket(bucket_name)
        command = "s3cmd put " + file_name + " s3://" + bucket_name
        self.call(command)

    def delete_file(self, file_name, bucket_name):
        command = "s3cmd del " + file_name + " s3://" + bucket_name
        self.call(command)

    def get_file(self, file_name, bucket_name, local_file=""):
        local_file = "riak_cs_" + file_name + "_" + helpers.now()
        command = "s3cmd get s3://" + bucket_name + "/" + file_name + " " + local_file
        self.call(command)


if __name__ == "__main__":
    cs = RiakCSConnector()

    bucket = "bucket.testing.2013.04.03"
    cs.create_bucket(bucket)
    cs.upload_file("salva", bucket)
    print cs.list_buckets_names()
    print cs.get_filenames_from_buckets()
    print cs.get_filenames_from_bucket("bucket.testing.salva")
