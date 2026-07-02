import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb'
import { DeleteObjectCommand, ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3'
import { PrismaClient } from '@prisma/client/extension'
import { ScheduledEvent } from 'aws-lambda/trigger/cloudwatch-events'

const prisma = new PrismaClient()
const s3CLient = new S3Client()
const dynamoClient = new DynamoDBClient()

export const handler = async (
	event: ScheduledEvent,
): Promise<{ statusCode: number; body: string }> => {
	console.log('Cron job started at:', event.time)

	const rawBucket = process.env.RAW_BUCKET_NAME
	const statsTable = process.env.STATS_TABLE_NAME

	if (!rawBucket || !statsTable) {
		throw new Error('Missing required environment variables.')
	}

	try {
		const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000)
		const stuckVideos = await prisma.video.findMany({
			where: {
				status: 'PENDING',
				createdAt: { lt: oneDayAgo },
			},
		})
		console.log(`Found ${stuckVideos.length} stuck PENDING videos to clean up.`)

		for (const video of stuckVideos) {
			try {
				const listCommand = new ListObjectsV2Command({
					Bucket: rawBucket,
					Prefix: video.id,
				})
				const s3Objects = await s3CLient.send(listCommand)
				if (s3Objects.Contents) {
					for (const item of s3Objects.Contents) {
						if (item.Key) {
							await s3CLient.send(
								new DeleteObjectCommand({
									Bucket: rawBucket,
									Key: item.Key,
								}),
							)
							console.log(`Deleted orphan S3 object: ${item.Key}`)
						}
					}
				}
			} catch (s3Error) {
				console.error(`Failed to clean S3 for video ${video.id}:`, s3Error)
			}
			console.log(`Deleting stuck video from database: ${video.id}`)
			await prisma.video.delete({ where: { id: video.id } })
		}

		const totalVideosToday = await prisma.video.count({
			where: {
				createdAt: { gte: oneDayAgo },
			},
		})

		const readyVideosToday = await prisma.video.count({
			where: {
				status: 'READY',
				createdAt: { gte: oneDayAgo },
			},
		})

		console.log(
			`Analytics: Uploaded today: ${totalVideosToday}, Processed successfully: ${readyVideosToday}`,
		)

		const todayIso = new Date().toISOString().split('T')[0]
		await dynamoClient.send(
			new PutItemCommand({
				TableName: statsTable,
				Item: {
					pk: { S: 'STATS' },
					sk: { S: `DAILY#${todayIso}` },
					uploadedCount: { N: totalVideosToday.toString() },
					processedCount: { N: readyVideosToday.toString() },
					cleanedCount: { N: stuckVideos.length.toString() },
					updatedAt: { S: new Date().toISOString() },
				},
			}),
		)
		console.log('Analytics successfully saved to DynamoDB.')
		return { statusCode: 200, body: 'Cleanup and ETL pipeline executed successfully.' }
	} catch (error) {
		console.error('Error executing cron jobs:', error)
		throw error
	} finally {
		await prisma.$disconnect()
	}
}
