DROP TABLE IF EXISTS `rustcdc`;

CREATE TABLE `rustcdc` (
    `id` INT UNSIGNED AUTO_INCREMENT,
    `title` VARCHAR(40) NOT NULL,
    PRIMARY KEY (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `rustcdc` (`title`) VALUES ('cdc-1');
