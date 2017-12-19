from assignment3.coordinator import Job
from assignment3 import mapper

if __name__ == "__main__":
    Job(mapper_path="assignment4/mr_apps/df_mapper.py",
        partitioner_type=mapper.DIGEST_PARTITIONER_TYPE,
        reducer_path="assignment4/mr_apps/df_reducer.py",
        num_reducers=1,
        job_path="assignment4/idf_jobs/").run()
    Job(mapper_path="assignment4/mr_apps/invindex_mapper.py",
        partitioner_type=mapper.INTEGER_PARTITIONER_TYPE,
        reducer_path="assignment4/mr_apps/invindex_reducer.py",
        num_reducers=3,
        job_path="assignment4/invindex_jobs/").run()
    Job(mapper_path="assignment4/mr_apps/docs_mapper.py",
        partitioner_type=mapper.INTEGER_PARTITIONER_TYPE,
        reducer_path="assignment4/mr_apps/docs_reducer.py",
        num_reducers=3,
        job_path="assignment4/docs_jobs/").run()
