import os
import boto3
import cv2
import json
from datetime import datetime, timedelta, timezone


def lambda_handler(event, context):
    # ������̉摜�������o�P�b�g���i�ċA�I�Ăяo�����|���̂ŁA��������ʃo�P�b�g�ɕۑ�����j
    output_bucket_name = os.environ['output_bucket_name']

    s3 = boto3.resource('s3')

    # S3�ɃA�b�v���ꂽ�摜�̏����擾����B
    input_bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    file_name = os.path.basename(object_key)

    # �摜��Rekognition�ŉ�͂���B
    rekognition = boto3.client('rekognition')
    response = rekognition.detect_labels(Image={
        'S3Object': {
            'Bucket': input_bucket_name,
            'Name': object_key
        }
    })

    # �l�����J�E���g����
    cnt = 0

    # S3����摜���_�E�����[�h����B
    tmp_dir = os.getenv('TMP_DIR', '/tmp/')
    bucket = s3.Bucket(input_bucket_name)
    bucket.download_file(object_key, tmp_dir + file_name)

    # ���o�����l���ɘg��`�悷��B
    image = cv2.imread(tmp_dir + file_name)
    height, width = image.shape[:2]

    for label in response['Labels']:
        if label['Name'] not in ['People', 'Person', 'Human']:
            continue
        for person in label['Instances']:
            cnt += 1
            box = person['BoundingBox']
            x = round(width * box['Left'])
            y = round(height * box['Top'])
            w = round(width * box['Width'])
            h = round(height * box['Height'])
            cv2.rectangle(image, (x, y), (x + w, y + h), (255, 255, 255), 3)

    cv2.imwrite(tmp_dir + file_name, image)

    # �`���̉摜��S3�ɃA�b�v���[�h����B
    outputBucket = s3.Bucket(output_bucket_name)
    outputBucket.upload_file(tmp_dir + file_name, file_name)
    os.remove(tmp_dir + file_name)

    # �l����\������
    print('���o���ꂽ�l��: {} �l'.format(cnt))
    
    # DynamoDB�ɐl����o�^����
    dynamodb = boto3.resource('dynamodb')
    
    #�w��e�[�u���̃A�N�Z�X�I�u�W�F�N�g�擾
    table = dynamodb.Table(os.environ['table_name'])
    
    #DynamoDB�Ƀ��R�[�h�ǉ�
    putResponse = table.put_item(
        Item={
        "MeasureDateTime": datetime.now(timezone(timedelta(hours=+9), 'JST')).strftime('%Y%m%d%H%M%S'),
        "value": cnt,
        "fileName": file_name
        }
    )