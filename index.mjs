'use strict';

import pg from 'pg';
const { Client } = pg;

import { SQSClient, DeleteMessageCommand } from '@aws-sdk/client-sqs';

import { v4 as uuidv4 } from 'uuid';

import AWS from 'aws-sdk';
const polly = new AWS.Polly({
    signatureVersion: 'v4',
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.ACCESS_KEY,
        secretAccessKey: process.env.SECRET_KEY
    }
});

const S3 = new AWS.S3({
    signatureVersion: 'v4',
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.ACCESS_KEY,
        secretAccessKey: process.env.SECRET_KEY
    }
});

const client = new Client({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    schema: process.env.DB_SCHEMA,
    port: process.env.DB_PORT,
    ssl: {
        rejectUnauthorized: false
    }
});

client.connect();

const sqsClient = new SQSClient({
    signatureVersion: 'v4',
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.ACCESS_KEY,
        secretAccessKey: process.env.SECRET_KEY
    }
});

const language = {
    'Arabic': 'arb',
    'Danish': 'da-DK',
    'Dutch': 'nl-NL',
    'English': 'en-US',
    'French': 'fr-FR',
    'German': 'de-DE',
    'Hindi': 'hi-IN',
    'Italian': 'it-IT',
    'Japanese': 'ja-JP',
    'Korean': 'ko-KR',
    'Portuguese': 'pt-PT',
    'Russian': 'ru-RU',
    'Spanish': 'es-ES',
    'Swedish': 'sv-SE',
    'Turkish': 'tr-TR'
}

export const handler = async (event, context) => {
    console.log('Event: ' + JSON.stringify(event));
    console.log('Context: ' + JSON.stringify(context));
    try {
        let messageId = parseInt(event['Records'][0]['body']);
        let message = await getMessage(messageId);
        let generated = await polly.synthesizeSpeech({
            LanguageCode: language[message.language_to],
            Text: message.translated,
            OutputFormat: 'mp3',
            VoiceId: 'Joanna'
        }).promise();
        let filename = uuidv4().replace(/-/g, '');
        const result = await S3.upload({
            Bucket: process.env.AWS_S3_BUCKET,
            Key: filename + '.mp3',
            Body: generated.AudioStream
        }).promise();
        updateMessage(messageId, filename + '.mp3');
        await sqsClient.send(new DeleteMessageCommand({
            QueueUrl: process.env.AWS_SQS_GENERATE,
            ReceiptHandle: event.Records[0].receiptHandle
        }));
    } catch (error) {
        console.error(error);
    }
};

const getMessage = async (messageId) => {
    let result = await client.query('select id, from_, to_, language_from, language_to, source, source_content_type, transcribed, translated, created, modified from lingualol.message where id = $1',
        [messageId]);
    console.log(result.rows);
    return result.rows[0];
}

const updateMessage = async (id, filename) => {
    await client.query('update lingualol.message set target = $1 where id = $2', [filename, id]);
}