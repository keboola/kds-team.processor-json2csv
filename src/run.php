<?php

declare(strict_types = 1);

use Keboola\Component\UserException;
use Keboola\Component\Logger;
use esnerda\Json2CsvProcessor\Component;

require __DIR__ . '/../vendor/autoload.php';

$logger = new Logger();
try {
    if (count($argv) > 1) {
        putenv("KBC_DATADIR=$argv[1]");
    }
    $app = new Component($logger);
    $app->execute();
    $logger->info("Conversion finished..");
    exit(0);
} catch (UserException $e) {
    $logger->error($e->getMessage());
    exit(1);
} catch (\Throwable $e) {
    $logger->critical(
            get_class($e) . ':' . $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'errCode' => $e->getCode(),
        'errTrace' => $e->getTraceAsString(),
        'errPrevious' => $e->getPrevious() ? get_class($e->getPrevious()) : '',
            ]
    );
    exit(1);
}
