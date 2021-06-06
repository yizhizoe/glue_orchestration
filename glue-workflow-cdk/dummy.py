import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME'])
print("This is dummy job: {}.".format(args['JOB_NAME']))
