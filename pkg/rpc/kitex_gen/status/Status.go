// Code generated by thriftgo (0.3.13). DO NOT EDIT.

package status

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"strings"
)

type TStatusCode int64

const (
	TStatusCode_OK                              TStatusCode = 0
	TStatusCode_CANCELLED                       TStatusCode = 1
	TStatusCode_ANALYSIS_ERROR                  TStatusCode = 2
	TStatusCode_NOT_IMPLEMENTED_ERROR           TStatusCode = 3
	TStatusCode_RUNTIME_ERROR                   TStatusCode = 4
	TStatusCode_MEM_LIMIT_EXCEEDED              TStatusCode = 5
	TStatusCode_INTERNAL_ERROR                  TStatusCode = 6
	TStatusCode_THRIFT_RPC_ERROR                TStatusCode = 7
	TStatusCode_TIMEOUT                         TStatusCode = 8
	TStatusCode_LIMIT_REACH                     TStatusCode = 9
	TStatusCode_MEM_ALLOC_FAILED                TStatusCode = 11
	TStatusCode_BUFFER_ALLOCATION_FAILED        TStatusCode = 12
	TStatusCode_MINIMUM_RESERVATION_UNAVAILABLE TStatusCode = 13
	TStatusCode_PUBLISH_TIMEOUT                 TStatusCode = 14
	TStatusCode_LABEL_ALREADY_EXISTS            TStatusCode = 15
	TStatusCode_TOO_MANY_TASKS                  TStatusCode = 16
	TStatusCode_END_OF_FILE                     TStatusCode = 30
	TStatusCode_NOT_FOUND                       TStatusCode = 31
	TStatusCode_CORRUPTION                      TStatusCode = 32
	TStatusCode_INVALID_ARGUMENT                TStatusCode = 33
	TStatusCode_IO_ERROR                        TStatusCode = 34
	TStatusCode_ALREADY_EXIST                   TStatusCode = 35
	TStatusCode_NETWORK_ERROR                   TStatusCode = 36
	TStatusCode_ILLEGAL_STATE                   TStatusCode = 37
	TStatusCode_NOT_AUTHORIZED                  TStatusCode = 38
	TStatusCode_ABORTED                         TStatusCode = 39
	TStatusCode_UNINITIALIZED                   TStatusCode = 42
	TStatusCode_INCOMPLETE                      TStatusCode = 44
	TStatusCode_OLAP_ERR_VERSION_ALREADY_MERGED TStatusCode = 45
	TStatusCode_DATA_QUALITY_ERROR              TStatusCode = 46
	TStatusCode_INVALID_JSON_PATH               TStatusCode = 47
	TStatusCode_BINLOG_DISABLE                  TStatusCode = 60
	TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ       TStatusCode = 61
	TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ       TStatusCode = 62
	TStatusCode_BINLOG_NOT_FOUND_DB             TStatusCode = 63
	TStatusCode_BINLOG_NOT_FOUND_TABLE          TStatusCode = 64
	TStatusCode_SNAPSHOT_NOT_EXIST              TStatusCode = 70
	TStatusCode_HTTP_ERROR                      TStatusCode = 71
	TStatusCode_TABLET_MISSING                  TStatusCode = 72
	TStatusCode_NOT_MASTER                      TStatusCode = 73
	TStatusCode_OBTAIN_LOCK_FAILED              TStatusCode = 74
	TStatusCode_SNAPSHOT_EXPIRED                TStatusCode = 75
	TStatusCode_DELETE_BITMAP_LOCK_ERROR        TStatusCode = 100
)

func (p TStatusCode) String() string {
	switch p {
	case TStatusCode_OK:
		return "OK"
	case TStatusCode_CANCELLED:
		return "CANCELLED"
	case TStatusCode_ANALYSIS_ERROR:
		return "ANALYSIS_ERROR"
	case TStatusCode_NOT_IMPLEMENTED_ERROR:
		return "NOT_IMPLEMENTED_ERROR"
	case TStatusCode_RUNTIME_ERROR:
		return "RUNTIME_ERROR"
	case TStatusCode_MEM_LIMIT_EXCEEDED:
		return "MEM_LIMIT_EXCEEDED"
	case TStatusCode_INTERNAL_ERROR:
		return "INTERNAL_ERROR"
	case TStatusCode_THRIFT_RPC_ERROR:
		return "THRIFT_RPC_ERROR"
	case TStatusCode_TIMEOUT:
		return "TIMEOUT"
	case TStatusCode_LIMIT_REACH:
		return "LIMIT_REACH"
	case TStatusCode_MEM_ALLOC_FAILED:
		return "MEM_ALLOC_FAILED"
	case TStatusCode_BUFFER_ALLOCATION_FAILED:
		return "BUFFER_ALLOCATION_FAILED"
	case TStatusCode_MINIMUM_RESERVATION_UNAVAILABLE:
		return "MINIMUM_RESERVATION_UNAVAILABLE"
	case TStatusCode_PUBLISH_TIMEOUT:
		return "PUBLISH_TIMEOUT"
	case TStatusCode_LABEL_ALREADY_EXISTS:
		return "LABEL_ALREADY_EXISTS"
	case TStatusCode_TOO_MANY_TASKS:
		return "TOO_MANY_TASKS"
	case TStatusCode_END_OF_FILE:
		return "END_OF_FILE"
	case TStatusCode_NOT_FOUND:
		return "NOT_FOUND"
	case TStatusCode_CORRUPTION:
		return "CORRUPTION"
	case TStatusCode_INVALID_ARGUMENT:
		return "INVALID_ARGUMENT"
	case TStatusCode_IO_ERROR:
		return "IO_ERROR"
	case TStatusCode_ALREADY_EXIST:
		return "ALREADY_EXIST"
	case TStatusCode_NETWORK_ERROR:
		return "NETWORK_ERROR"
	case TStatusCode_ILLEGAL_STATE:
		return "ILLEGAL_STATE"
	case TStatusCode_NOT_AUTHORIZED:
		return "NOT_AUTHORIZED"
	case TStatusCode_ABORTED:
		return "ABORTED"
	case TStatusCode_UNINITIALIZED:
		return "UNINITIALIZED"
	case TStatusCode_INCOMPLETE:
		return "INCOMPLETE"
	case TStatusCode_OLAP_ERR_VERSION_ALREADY_MERGED:
		return "OLAP_ERR_VERSION_ALREADY_MERGED"
	case TStatusCode_DATA_QUALITY_ERROR:
		return "DATA_QUALITY_ERROR"
	case TStatusCode_INVALID_JSON_PATH:
		return "INVALID_JSON_PATH"
	case TStatusCode_BINLOG_DISABLE:
		return "BINLOG_DISABLE"
	case TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ:
		return "BINLOG_TOO_OLD_COMMIT_SEQ"
	case TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ:
		return "BINLOG_TOO_NEW_COMMIT_SEQ"
	case TStatusCode_BINLOG_NOT_FOUND_DB:
		return "BINLOG_NOT_FOUND_DB"
	case TStatusCode_BINLOG_NOT_FOUND_TABLE:
		return "BINLOG_NOT_FOUND_TABLE"
	case TStatusCode_SNAPSHOT_NOT_EXIST:
		return "SNAPSHOT_NOT_EXIST"
	case TStatusCode_HTTP_ERROR:
		return "HTTP_ERROR"
	case TStatusCode_TABLET_MISSING:
		return "TABLET_MISSING"
	case TStatusCode_NOT_MASTER:
		return "NOT_MASTER"
	case TStatusCode_OBTAIN_LOCK_FAILED:
		return "OBTAIN_LOCK_FAILED"
	case TStatusCode_SNAPSHOT_EXPIRED:
		return "SNAPSHOT_EXPIRED"
	case TStatusCode_DELETE_BITMAP_LOCK_ERROR:
		return "DELETE_BITMAP_LOCK_ERROR"
	}
	return "<UNSET>"
}

func TStatusCodeFromString(s string) (TStatusCode, error) {
	switch s {
	case "OK":
		return TStatusCode_OK, nil
	case "CANCELLED":
		return TStatusCode_CANCELLED, nil
	case "ANALYSIS_ERROR":
		return TStatusCode_ANALYSIS_ERROR, nil
	case "NOT_IMPLEMENTED_ERROR":
		return TStatusCode_NOT_IMPLEMENTED_ERROR, nil
	case "RUNTIME_ERROR":
		return TStatusCode_RUNTIME_ERROR, nil
	case "MEM_LIMIT_EXCEEDED":
		return TStatusCode_MEM_LIMIT_EXCEEDED, nil
	case "INTERNAL_ERROR":
		return TStatusCode_INTERNAL_ERROR, nil
	case "THRIFT_RPC_ERROR":
		return TStatusCode_THRIFT_RPC_ERROR, nil
	case "TIMEOUT":
		return TStatusCode_TIMEOUT, nil
	case "LIMIT_REACH":
		return TStatusCode_LIMIT_REACH, nil
	case "MEM_ALLOC_FAILED":
		return TStatusCode_MEM_ALLOC_FAILED, nil
	case "BUFFER_ALLOCATION_FAILED":
		return TStatusCode_BUFFER_ALLOCATION_FAILED, nil
	case "MINIMUM_RESERVATION_UNAVAILABLE":
		return TStatusCode_MINIMUM_RESERVATION_UNAVAILABLE, nil
	case "PUBLISH_TIMEOUT":
		return TStatusCode_PUBLISH_TIMEOUT, nil
	case "LABEL_ALREADY_EXISTS":
		return TStatusCode_LABEL_ALREADY_EXISTS, nil
	case "TOO_MANY_TASKS":
		return TStatusCode_TOO_MANY_TASKS, nil
	case "END_OF_FILE":
		return TStatusCode_END_OF_FILE, nil
	case "NOT_FOUND":
		return TStatusCode_NOT_FOUND, nil
	case "CORRUPTION":
		return TStatusCode_CORRUPTION, nil
	case "INVALID_ARGUMENT":
		return TStatusCode_INVALID_ARGUMENT, nil
	case "IO_ERROR":
		return TStatusCode_IO_ERROR, nil
	case "ALREADY_EXIST":
		return TStatusCode_ALREADY_EXIST, nil
	case "NETWORK_ERROR":
		return TStatusCode_NETWORK_ERROR, nil
	case "ILLEGAL_STATE":
		return TStatusCode_ILLEGAL_STATE, nil
	case "NOT_AUTHORIZED":
		return TStatusCode_NOT_AUTHORIZED, nil
	case "ABORTED":
		return TStatusCode_ABORTED, nil
	case "UNINITIALIZED":
		return TStatusCode_UNINITIALIZED, nil
	case "INCOMPLETE":
		return TStatusCode_INCOMPLETE, nil
	case "OLAP_ERR_VERSION_ALREADY_MERGED":
		return TStatusCode_OLAP_ERR_VERSION_ALREADY_MERGED, nil
	case "DATA_QUALITY_ERROR":
		return TStatusCode_DATA_QUALITY_ERROR, nil
	case "INVALID_JSON_PATH":
		return TStatusCode_INVALID_JSON_PATH, nil
	case "BINLOG_DISABLE":
		return TStatusCode_BINLOG_DISABLE, nil
	case "BINLOG_TOO_OLD_COMMIT_SEQ":
		return TStatusCode_BINLOG_TOO_OLD_COMMIT_SEQ, nil
	case "BINLOG_TOO_NEW_COMMIT_SEQ":
		return TStatusCode_BINLOG_TOO_NEW_COMMIT_SEQ, nil
	case "BINLOG_NOT_FOUND_DB":
		return TStatusCode_BINLOG_NOT_FOUND_DB, nil
	case "BINLOG_NOT_FOUND_TABLE":
		return TStatusCode_BINLOG_NOT_FOUND_TABLE, nil
	case "SNAPSHOT_NOT_EXIST":
		return TStatusCode_SNAPSHOT_NOT_EXIST, nil
	case "HTTP_ERROR":
		return TStatusCode_HTTP_ERROR, nil
	case "TABLET_MISSING":
		return TStatusCode_TABLET_MISSING, nil
	case "NOT_MASTER":
		return TStatusCode_NOT_MASTER, nil
	case "OBTAIN_LOCK_FAILED":
		return TStatusCode_OBTAIN_LOCK_FAILED, nil
	case "SNAPSHOT_EXPIRED":
		return TStatusCode_SNAPSHOT_EXPIRED, nil
	case "DELETE_BITMAP_LOCK_ERROR":
		return TStatusCode_DELETE_BITMAP_LOCK_ERROR, nil
	}
	return TStatusCode(0), fmt.Errorf("not a valid TStatusCode string")
}

func TStatusCodePtr(v TStatusCode) *TStatusCode { return &v }
func (p *TStatusCode) Scan(value interface{}) (err error) {
	var result sql.NullInt64
	err = result.Scan(value)
	*p = TStatusCode(result.Int64)
	return
}

func (p *TStatusCode) Value() (driver.Value, error) {
	if p == nil {
		return nil, nil
	}
	return int64(*p), nil
}

type TStatus struct {
	StatusCode TStatusCode `thrift:"status_code,1,required" frugal:"1,required,TStatusCode" json:"status_code"`
	ErrorMsgs  []string    `thrift:"error_msgs,2,optional" frugal:"2,optional,list<string>" json:"error_msgs,omitempty"`
}

func NewTStatus() *TStatus {
	return &TStatus{}
}

func (p *TStatus) InitDefault() {
}

func (p *TStatus) GetStatusCode() (v TStatusCode) {
	return p.StatusCode
}

var TStatus_ErrorMsgs_DEFAULT []string

func (p *TStatus) GetErrorMsgs() (v []string) {
	if !p.IsSetErrorMsgs() {
		return TStatus_ErrorMsgs_DEFAULT
	}
	return p.ErrorMsgs
}
func (p *TStatus) SetStatusCode(val TStatusCode) {
	p.StatusCode = val
}
func (p *TStatus) SetErrorMsgs(val []string) {
	p.ErrorMsgs = val
}

var fieldIDToName_TStatus = map[int16]string{
	1: "status_code",
	2: "error_msgs",
}

func (p *TStatus) IsSetErrorMsgs() bool {
	return p.ErrorMsgs != nil
}

func (p *TStatus) Read(iprot thrift.TProtocol) (err error) {

	var fieldTypeId thrift.TType
	var fieldId int16
	var issetStatusCode bool = false

	if _, err = iprot.ReadStructBegin(); err != nil {
		goto ReadStructBeginError
	}

	for {
		_, fieldTypeId, fieldId, err = iprot.ReadFieldBegin()
		if err != nil {
			goto ReadFieldBeginError
		}
		if fieldTypeId == thrift.STOP {
			break
		}

		switch fieldId {
		case 1:
			if fieldTypeId == thrift.I32 {
				if err = p.ReadField1(iprot); err != nil {
					goto ReadFieldError
				}
				issetStatusCode = true
			} else if err = iprot.Skip(fieldTypeId); err != nil {
				goto SkipFieldError
			}
		case 2:
			if fieldTypeId == thrift.LIST {
				if err = p.ReadField2(iprot); err != nil {
					goto ReadFieldError
				}
			} else if err = iprot.Skip(fieldTypeId); err != nil {
				goto SkipFieldError
			}
		default:
			if err = iprot.Skip(fieldTypeId); err != nil {
				goto SkipFieldError
			}
		}
		if err = iprot.ReadFieldEnd(); err != nil {
			goto ReadFieldEndError
		}
	}
	if err = iprot.ReadStructEnd(); err != nil {
		goto ReadStructEndError
	}

	if !issetStatusCode {
		fieldId = 1
		goto RequiredFieldNotSetError
	}
	return nil
ReadStructBeginError:
	return thrift.PrependError(fmt.Sprintf("%T read struct begin error: ", p), err)
ReadFieldBeginError:
	return thrift.PrependError(fmt.Sprintf("%T read field %d begin error: ", p, fieldId), err)
ReadFieldError:
	return thrift.PrependError(fmt.Sprintf("%T read field %d '%s' error: ", p, fieldId, fieldIDToName_TStatus[fieldId]), err)
SkipFieldError:
	return thrift.PrependError(fmt.Sprintf("%T field %d skip type %d error: ", p, fieldId, fieldTypeId), err)

ReadFieldEndError:
	return thrift.PrependError(fmt.Sprintf("%T read field end error", p), err)
ReadStructEndError:
	return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
RequiredFieldNotSetError:
	return thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, fmt.Errorf("required field %s is not set", fieldIDToName_TStatus[fieldId]))
}

func (p *TStatus) ReadField1(iprot thrift.TProtocol) error {

	var _field TStatusCode
	if v, err := iprot.ReadI32(); err != nil {
		return err
	} else {
		_field = TStatusCode(v)
	}
	p.StatusCode = _field
	return nil
}
func (p *TStatus) ReadField2(iprot thrift.TProtocol) error {
	_, size, err := iprot.ReadListBegin()
	if err != nil {
		return err
	}
	_field := make([]string, 0, size)
	for i := 0; i < size; i++ {

		var _elem string
		if v, err := iprot.ReadString(); err != nil {
			return err
		} else {
			_elem = v
		}

		_field = append(_field, _elem)
	}
	if err := iprot.ReadListEnd(); err != nil {
		return err
	}
	p.ErrorMsgs = _field
	return nil
}

func (p *TStatus) Write(oprot thrift.TProtocol) (err error) {
	var fieldId int16
	if err = oprot.WriteStructBegin("TStatus"); err != nil {
		goto WriteStructBeginError
	}
	if p != nil {
		if err = p.writeField1(oprot); err != nil {
			fieldId = 1
			goto WriteFieldError
		}
		if err = p.writeField2(oprot); err != nil {
			fieldId = 2
			goto WriteFieldError
		}
	}
	if err = oprot.WriteFieldStop(); err != nil {
		goto WriteFieldStopError
	}
	if err = oprot.WriteStructEnd(); err != nil {
		goto WriteStructEndError
	}
	return nil
WriteStructBeginError:
	return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
WriteFieldError:
	return thrift.PrependError(fmt.Sprintf("%T write field %d error: ", p, fieldId), err)
WriteFieldStopError:
	return thrift.PrependError(fmt.Sprintf("%T write field stop error: ", p), err)
WriteStructEndError:
	return thrift.PrependError(fmt.Sprintf("%T write struct end error: ", p), err)
}

func (p *TStatus) writeField1(oprot thrift.TProtocol) (err error) {
	if err = oprot.WriteFieldBegin("status_code", thrift.I32, 1); err != nil {
		goto WriteFieldBeginError
	}
	if err := oprot.WriteI32(int32(p.StatusCode)); err != nil {
		return err
	}
	if err = oprot.WriteFieldEnd(); err != nil {
		goto WriteFieldEndError
	}
	return nil
WriteFieldBeginError:
	return thrift.PrependError(fmt.Sprintf("%T write field 1 begin error: ", p), err)
WriteFieldEndError:
	return thrift.PrependError(fmt.Sprintf("%T write field 1 end error: ", p), err)
}

func (p *TStatus) writeField2(oprot thrift.TProtocol) (err error) {
	if p.IsSetErrorMsgs() {
		if err = oprot.WriteFieldBegin("error_msgs", thrift.LIST, 2); err != nil {
			goto WriteFieldBeginError
		}
		if err := oprot.WriteListBegin(thrift.STRING, len(p.ErrorMsgs)); err != nil {
			return err
		}
		for _, v := range p.ErrorMsgs {
			if err := oprot.WriteString(v); err != nil {
				return err
			}
		}
		if err := oprot.WriteListEnd(); err != nil {
			return err
		}
		if err = oprot.WriteFieldEnd(); err != nil {
			goto WriteFieldEndError
		}
	}
	return nil
WriteFieldBeginError:
	return thrift.PrependError(fmt.Sprintf("%T write field 2 begin error: ", p), err)
WriteFieldEndError:
	return thrift.PrependError(fmt.Sprintf("%T write field 2 end error: ", p), err)
}

func (p *TStatus) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("TStatus(%+v)", *p)

}

func (p *TStatus) DeepEqual(ano *TStatus) bool {
	if p == ano {
		return true
	} else if p == nil || ano == nil {
		return false
	}
	if !p.Field1DeepEqual(ano.StatusCode) {
		return false
	}
	if !p.Field2DeepEqual(ano.ErrorMsgs) {
		return false
	}
	return true
}

func (p *TStatus) Field1DeepEqual(src TStatusCode) bool {

	if p.StatusCode != src {
		return false
	}
	return true
}
func (p *TStatus) Field2DeepEqual(src []string) bool {

	if len(p.ErrorMsgs) != len(src) {
		return false
	}
	for i, v := range p.ErrorMsgs {
		_src := src[i]
		if strings.Compare(v, _src) != 0 {
			return false
		}
	}
	return true
}
