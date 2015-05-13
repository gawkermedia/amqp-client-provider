ALTER TABLE `rabbit_messages` ADD `processedBy` VARCHAR(255)
 NULL
 DEFAULT NULL
 AFTER `createdTime`;

ALTER TABLE `rabbit_messages` ADD `lockedAt` TIMESTAMP
  NULL
AFTER `processedBy`;

ALTER TABLE `rabbit_messages` DROP INDEX `channelId`;

ALTER TABLE `rabbit_messages` DROP INDEX `deliveryTag`;

ALTER TABLE `rabbit_messages` ADD INDEX `channelId_deliveryTag` (`channelId`, `deliveryTag`);

ALTER TABLE `rabbit_messages` ADD INDEX `processedBy_lockedAt` (`processedBy`, `lockedAt`);

ALTER TABLE `rabbit_confirmations` DROP INDEX `channelId`;

ALTER TABLE `rabbit_confirmations` DROP INDEX `deliveryTag`;

ALTER TABLE `rabbit_confirmations` ADD INDEX `channelId_deliveryTag` (`channelId`, `deliveryTag`);

