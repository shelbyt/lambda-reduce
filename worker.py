import boto3

S3_BUCKET = "shelby-lambda-in"


def wordCount(data):
	# assuming the data is a long string
	wordList = data.split(' ')
	wordDict = {}
	for word in wordList:
		if wordDict.has_key(word):
			wordDict[word] += 1
		else:
			wordDict[word] = 1

	return wordDict.items()


def readData(filename, start, end):
	if end < start:
		raise Exception("Invalid input for data read")

    client = boto3.client("s3")
    result = client.get_object(
        Bucket = S3_BUCKET,
        Key = filename,
        Range = "bytes=" + start + "-" + end
    )

	# read the entire body
	return result['Body'].read()


def writeData(data, index):
	tmpDataFile = "tmp" + str(index) + ".txt"

	body = ""
	for item in data:
		body += str(item) + '\n'

	client = boto3.client("s3")
	result = client.put_object(
		Bucket = S3_BUCKET,
		Key = tmpDataFile,
		Body = body
	)
	return


if __name__ == "__main__":
	# read data from s3 bucket
	data = readData(FileName, StartOffset, EndOffset)

	# do the calculation
	output = wordCount(data)

	# write the data back to s3 bucket
	writeData(output, InstanceIndex)