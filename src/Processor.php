<?php

namespace esnerda\Json2CsvProcessor;

use Keboola\Component\Manifest\ManifestManager;
use Keboola\CsvTable\Table;
use Psr\Log\LoggerInterface;
use Symfony\Component\Finder\Finder;

class Processor
{
    const FILE_NAME_COL_NAME = 'keboola_file_name_col';

    public function __construct(
        private JsonToCSvParser $jsonParser,
        private ManifestManager $manifestManager,
        private bool $add_row_nr,
        private bool $incremental,
        private string $root_el,
        private bool $addFileName,
        private bool $usingLegacyManifest,
        private LoggerInterface $logger,
    )
    {
    }

    public function convert(string $datadir, string $type): void
    {
        $this->processFiles(
            sprintf("%s/in/" . $type . '/', $datadir),
            sprintf("%s/out/tables/", $datadir)
        );
    }

    private function processFiles(string $inputDir, string $outputDir): void
    {
        $finderFiles = new Finder();
        
        $finderFiles->files()->in($inputDir)->notName('*.manifest');
        $finderFiles->sortByName();

        foreach ($finderFiles as $file) {
            $this->logger->info("Parsing file " . $file->getFileName());

            $json_string = file_get_contents($file->getRealPath());
            $json_content = json_decode($json_string);

            // get root if specified
            $json_result_root = $this->getRoot($json_content);
            // add file name col
            if ($this->addFileName) {
                $json_result_root = $this->addFileName(json_encode($json_result_root), $file->getFileName(), self::FILE_NAME_COL_NAME);
            }

            $this->jsonParser->parse($json_result_root);
        }

        $this->logger->info("Writting results..");
        $this->storeResults(
            $outputDir,
            $this->jsonParser->getCsvFiles(),
            $this->incremental
        );
    }

    private function addFileName($json, $fileName, $colName)
    {
        // convert to arrays
        $json_arr = json_decode($json, true);
        // add filename col to root
        if (!is_array($json_arr)) {
            $json_arr[$colName] = $fileName;
        } else {
            // if its array, add field to all members
            foreach ($json_arr as $key => $entry) {
                $json_arr[$key][$colName] = $fileName;
            }
        }
        return json_decode(json_encode($json_arr));
    }
    private function getRoot($json)
    {
        if ($this->root_el != null) {
            $nodes = explode('.', $this->root_el);
            $root = $json;
            foreach ($nodes as $node) {
                $root = $root->{$node};
            }
            return $root;
        } else {
            return $json;
        }
    }

    /**
     * @param Table[] $csvFiles
     */
    private function storeResults(string $outdir, array $csvFiles, bool $incremental = false): void
    {
        foreach ($csvFiles as $key => $file) {
            $path = $outdir;

            if ($file == null) {
                $this->logger->info("No results parsed.");
                return;
            }

            if (!is_dir($path)) {
                mkdir($path, null, true);
                chown($path, fileowner($outdir));
                chgrp($path, filegroup($outdir));
            }

            $resFileName = $key . '.csv';

            $manifestNew = $this->manifestManager->getTableManifest($resFileName);
            $manifestNew->setIncremental($file->isIncrementalSet() ? $file->getIncremental() : $incremental);
            if (!empty($file->getPrimaryKey())) {
                $manifestNew->setLegacyPrimaryKeys($file->getPrimaryKey(true));
            }

            $this->logger->info("Writing result file: " . $resFileName);
            $this->manifestManager->writeTableManifest($resFileName, $manifestNew, $this->usingLegacyManifest);
            copy($file->getPathname(), $path . $resFileName);
        }
    }
}
