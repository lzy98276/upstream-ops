// Package global 存放构建版本等全局常量。
package global

// VERSION is the source-build fallback. Release builds override it from the
// Git tag through the linker so publishing does not require editing this file.
var VERSION = "0.1.1"
