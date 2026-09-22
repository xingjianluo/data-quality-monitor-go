// Package errs 定义归一化错误类型，保证异常在对外结果中以可归类的
// ReasonCode 呈现，而非直接透出底层错误。
package errs

import (
	"errors"
	"fmt"

	"github.com/xingjianluo/data-quality-monitor-go/internal/model"
)

// Error 是带原因分类的归一化错误。
type Error struct {
	Code    model.ReasonCode
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Code) + ": " + e.Message
}

// New 构造一个归一化错误。
func New(code model.ReasonCode, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf 提取错误的原因分类，未知错误归一化为 internal。
func CodeOf(err error) model.ReasonCode {
	var de *Error
	if errors.As(err, &de) {
		return de.Code
	}
	return model.ReasonInternal
}
