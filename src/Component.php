<?php

declare(strict_types = 1);

namespace esnerda\Json2CsvProcessor;

use Keboola\Component\BaseComponent;

class Component extends BaseComponent {

    protected function run(): void {
        $type = 'root';
        if ($this->getConfig()->getRootNode() != NULL) {
            $nodes = explode('.', $this->getConfig()->getRootNode());
            $type = $nodes[count($nodes) - 1];
        }

        $processor = new Processor(
            new JsonToCSvParser($this->getConfig()->getMapping(), $this->getLogger(), $type),
            $this->getManifestManager(),
            $this->getConfig()->getAppendRowNr(),
            $this->getConfig()->isIncremental(),
            $this->getConfig()->getRootNode(),
            $this->getConfig()->addFileName(),
            $this->getConfig()->getDataTypeSupport()->usingLegacyManifest(),
            $this->getLogger()
        );

        $processor->convert($this->getDataDir(), $this->getConfig()->getInputType());
    }

    protected function getConfigClass(): string {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string {
        return ConfigDefinition::class;
    }

}
