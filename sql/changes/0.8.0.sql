ALTER TABLE `rabbit_messages` ADD `processedBy` VARCHAR(256)
 NULL
 DEFAULT NULL
 AFTER `createdTime`;

ALTER TABLE `rabbit_messages` ADD `lockedAt` TIMESTAMP
  NULL
AFTER `processedBy`;

