import glob
import boto3
import os
import sys
def upload(f):
    s3.meta.client.upload_file(f, 'aws21-squeezebox-analysis-output', f)

# simply upload all the file with same prefix
# for f in glob.glob('log-*-*.txt'):
#  check argument input
if len(sys.argv) != 2:
    print 'too few or too many argument input.'
    print 'correct input format: python upload <month>. 01/02.. or 10/11..'
try:
    month = str(sys.argv[1])
except:
    print 'arg format not match.'
    sys.exit(1)

s3 = boto3.resource('s3')
for f in glob.glob('2016_'+ month + '_*.pickle'):
  print 'Now uploading: ', f
  upload(f)
  os.remove(f)
# upload('../calculated_output.json')