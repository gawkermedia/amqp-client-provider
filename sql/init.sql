CREATE TABLE `rabbit_messages` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `routingKey` varchar(256) NOT NULL,
  `exchangeName` varchar(128) NOT NULL,
  `message` text NOT NULL,
  `channelId` varchar(36) DEFAULT NULL,
  `deliveryTag` int(10) unsigned DEFAULT NULL,
  `createdTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `processedBy` varchar(256) DEFAULT NULL,
  `lockedAt` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `channelId` (`channelId`),
  KEY `deliveryTag` (`deliveryTag`),
  KEY `createdTime` (`createdTime`),
  KEY `exchangeName` (`exchangeName`,`createdTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `rabbit_confirmations` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `channelId` varchar(36) NOT NULL,
  `deliveryTag` int(10) unsigned NOT NULL,
  `multiple` tinyint(1) NOT NULL,
  `createdTime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `channelId` (`channelId`),
  KEY `multiple` (`multiple`),
  KEY `deliveryTag` (`deliveryTag`),
  KEY `createdTime` (`createdTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;