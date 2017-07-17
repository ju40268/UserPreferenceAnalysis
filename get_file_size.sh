for bucket in `aws s3 ls | awk '{print $3}'`
do 
   aws s3 ls s3://$bucket  --recursive | 
   awk -v bucket=$bucket 'BEGIN {total=0} 
   BEGIN {count=0} {total+=$3} {count++} 
   END {print bucket"\t"count"\t"total/1024/1024/1024}'
   
   aws s3api get-bucket-acl --bucket $bucket --output text
done