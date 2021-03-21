package com.verizon.dma.beans

import scala.beans.BeanProperty

class AppProperty() {
  @BeanProperty var appName: String = null
  @BeanProperty var mode: String = null
  @BeanProperty var env: String = null
  @BeanProperty var keyVaultScope: String = null
  @BeanProperty var basePath: String = null
  @BeanProperty var maxEventsPerTrigger: Int = 0
  @BeanProperty var coalesceCount: Int = 1
  @BeanProperty var parquetBlockSize: Int = 0
  @BeanProperty var consumergroup: String = null
  @BeanProperty var fromoffset: String = null




}