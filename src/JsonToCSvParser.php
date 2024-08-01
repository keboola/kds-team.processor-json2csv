<?php

namespace esnerda\Json2CsvProcessor;

use Keboola\CsvTable\Table;
use Keboola\Json\Analyzer;
use Keboola\Json\Parser;
use Keboola\Json\Structure;
use Keboola\CsvMap\Mapper;
use Psr\Log\LoggerInterface;

class JsonToCSvParser
{
    private LoggerInterface $logger;
    private $parser;
    private $type;

    public function __construct(array $mapping, $logger, $type)
    {
        $this->logger = $logger;
        $this->type = $type;
        if ($mapping) {
            $this->parser = new Mapper($mapping, type: $type);
        } else {
            $this->parser = new Parser(new Analyzer($logger, new Structure(), true));
        }
    }

    public function parse($jsonData)
    {
        $type = $this->getType($jsonData);
        if (!is_array($jsonData)) {
            $jsonData = [$jsonData];
        }
        if ($this->parser instanceof Mapper) {
            $this->parser->parse($jsonData);
        } else {
            $this->parser->process($jsonData, $type);
        }
    }

    public function getCsvFiles()
    {
        return $this->parser->getCsvFiles();
    }

    private function getType($jsonData)
    {
        $type = key((array) $jsonData);
        if (!is_string ($type)) {
            $type = 'root';
        }
        return $type;
    }
}
