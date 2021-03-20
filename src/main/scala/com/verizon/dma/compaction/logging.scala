package com.verizon.dma.compaction

import org.apache.log4j.Logger

trait Logging {

  protected val logger: Logger = Logger.getLogger(getClass.getName)

}