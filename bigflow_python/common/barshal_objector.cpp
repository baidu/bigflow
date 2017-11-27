/***************************************************************************
 *
 * Copyright (c) 2015 Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/
// Author: PanYunhong <bigflow-opensource@baidu.com>
//

#ifdef Py_PYTHON_H
#undef Py_PYTHON_H
#endif
#include "python2.7/Python.h"

#include "barshal_objector.h"
#include "code.h"
#include "longintrepr.h"

#include "bigflow_python/python_interpreter.h"
#include "bigflow_python/common/python.h"

namespace baidu {
namespace bigflow {
namespace python {

#define ABS(x) ((x) < 0 ? -(x) : (x))

/* High water mark to determine when the marshalled object is dangerously deep
 * and risks coring the interpreter.  When the object stack gets this deep,
 * raise an exception instead of continuing.
 */
const char TYPE_NULL = '0';
const char TYPE_NONE = 'N';
const char TYPE_FALSE = 'F';
const char TYPE_TRUE = 'T';
const char TYPE_STOPITER = 'S';
const char TYPE_ELLIPSIS = '.';
const char TYPE_INT = 'i';
const char TYPE_INT64 = 'I';
const char TYPE_FLOAT = 'f';
const char TYPE_BINARY_FLOAT = 'g';
const char TYPE_COMPLEX = 'x';
const char TYPE_BINARY_COMPLEX = 'y';
const char TYPE_LONG = 'l';
const char TYPE_STRING = 's';
const char TYPE_INTERNED = 't';
const char TYPE_STRINGREF = 'R';
const char TYPE_TUPLE = '(';
const char TYPE_LIST = '[';
const char TYPE_DICT = '{';
const char TYPE_CODE = 'c';
const char TYPE_UNICODE = 'u';
const char TYPE_UNKNOWN = '?';
const char TYPE_SET = '<';
const char TYPE_FROZENSET = '>';

const Py_ssize_t SIZE32_MAX = 0x7FFFFFFF;

/* We assume that Python longs are stored internally in base some power of
   2**15; for the sake of portability we'll always read and write them in base
   exactly 2**15. */

const Py_ssize_t PyLong_MARSHAL_SHIFT = 15;
const Py_ssize_t PyLong_MARSHAL_BASE = ((short)1 << PyLong_MARSHAL_SHIFT);
#define PyLong_MARSHAL_MASK (PyLong_MARSHAL_BASE - 1)
//#if PyLong_SHIFT % PyLong_MARSHAL_SHIFT != 0
//#error "PyLong_SHIFT must be a multiple of PyLong_MARSHAL_SHIFT"
//#endif
const Py_ssize_t PyLong_MARSHAL_RATIO = (PyLong_SHIFT / PyLong_MARSHAL_SHIFT);

static int cp_object(PyObject *v, char *buf, int size);

static int
cp_byte(char ch, char *buf, int size) {
    if (size >= 1) {
        *buf = ch;
    }
    return 1;
}

static int
cp_long(int x, char *buf, int size)
{
    int wcnt = 0;
    wcnt += cp_byte((char)(x       & 0xff), buf, size-wcnt);
    wcnt += cp_byte((char)((x>>8)  & 0xff), buf+wcnt, size-wcnt);
    wcnt += cp_byte((char)((x>>16) & 0xff), buf+wcnt, size-wcnt);
    wcnt += cp_byte((char)((x>>24) & 0xff), buf+wcnt, size-wcnt);
    return wcnt;
}

static int
cp_long64(long x, char *buf, int size)
{
    int wcnt = 0;
    wcnt += cp_long(x, buf, size-wcnt);
    wcnt += cp_long(x>>32, buf+wcnt, size-wcnt);
    return wcnt;
}

static int
cp_short(short x, char *buf, int size)
{
    int wcnt = 0;
    wcnt += cp_byte((char)(x & 0xff), buf, size-wcnt);
    wcnt += cp_byte((char)((x>>8) & 0xff), buf+wcnt, size-wcnt);
    return wcnt;
}

static int
cp_pylong(const PyLongObject *ob, char *buf, int size) {
    Py_ssize_t i;
    Py_ssize_t j;
    Py_ssize_t n;
    Py_ssize_t l;
    digit d;

    int wcnt = 0;

    wcnt += cp_byte(TYPE_LONG, buf+wcnt, size-wcnt);
    if (Py_SIZE(ob) == 0) {
        wcnt += cp_long(0, buf+wcnt, size-wcnt);
        return wcnt;
    }

    /* set l to number of base PyLong_MARSHAL_BASE digits */
    n = ABS(Py_SIZE(ob));
    l = (n-1) * PyLong_MARSHAL_RATIO;
    d = ob->ob_digit[n-1];
    assert(d != 0); /* a PyLong is always normalized */
    do {
        d >>= PyLong_MARSHAL_SHIFT;
        l++;
    } while (d != 0);
    BIGFLOW_PYTHON_RAISE_EXCEPTION_IF(l > SIZE32_MAX, "serialize failed for type <long>");
    wcnt += cp_long((int)(Py_SIZE(ob) > 0 ? l : -l), buf+wcnt, size-wcnt);

    for (i = 0; i < n - 1;++i) {
        d = ob->ob_digit[i];
        for (j = 0;j < PyLong_MARSHAL_RATIO;++j) {
            wcnt += cp_short(d & PyLong_MARSHAL_MASK, buf+wcnt, size-wcnt);
            d >>= PyLong_MARSHAL_SHIFT;
        }
        assert(d == 0);
    }
    d = ob->ob_digit[n-1];
    do {
        wcnt += cp_short(d & PyLong_MARSHAL_MASK, buf+wcnt, size-wcnt);
        d >>= PyLong_MARSHAL_SHIFT;
    } while (d != 0);

    return wcnt;
}

static int
cp_string(const char *s, Py_ssize_t n, char *buf, int size) {
    if (n <= size) {
        memcpy(buf, s, n);
    }
    return (int)n;
}

static int
cp_pstring(const char *s, Py_ssize_t n, char *buf, int size) {
    int wcnt = 0;
    wcnt += cp_long(n, buf+wcnt, size-wcnt);
    wcnt += cp_string(s, n, buf+wcnt, size-wcnt);
    return wcnt;
}

static int
cp_pyset(PyObject* v, char* buf, int size) {
    PyObject *value = NULL;
    PyObject *it = NULL;
    Py_ssize_t n;
    int wcnt = 0;

    if (PyObject_TypeCheck(v, &PySet_Type)) {
        wcnt += cp_byte(TYPE_SET, buf+wcnt, size-wcnt);
    }
    else {
        wcnt += cp_byte(TYPE_FROZENSET, buf+wcnt, size-wcnt);
    }
    n = PyObject_Size(v);
    BIGFLOW_PYTHON_RAISE_EXCEPTION_IF(n == -1, "PyObject_Size return -1");

    wcnt += cp_long(n, buf+wcnt, size-wcnt);
    it = PyObject_GetIter(v);

    BIGFLOW_PYTHON_RAISE_EXCEPTION_IF(it == NULL, "PyObject_GetIter return NULL");

    while ((value = PyIter_Next(it)) != NULL) {
        wcnt += cp_object(value, buf+wcnt, size-wcnt);
        Py_DECREF(value);
    }
    Py_DECREF(it);
    if (PyErr_Occurred()) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
    return wcnt;
}

static int
cp_object(PyObject *v, char *buf, int size) {
    int wcnt = 0;
    if (v == NULL) {
        wcnt += cp_byte(TYPE_NULL, buf, size);
    } else if (v == Py_None) {
        wcnt += cp_byte(TYPE_NONE, buf, size);
    } else if (v == Py_False) {
        wcnt += cp_byte(TYPE_FALSE, buf, size);
    } else if (v == Py_True) {
        wcnt += cp_byte(TYPE_TRUE, buf, size);
    } else if (PyInt_CheckExact(v)) {
        long x = PyInt_AS_LONG((PyIntObject *)v);
        long y = Py_ARITHMETIC_RIGHT_SHIFT(long, x, 31);
        if (y && y != -1) {
            wcnt += cp_byte(TYPE_INT64, buf, size-wcnt);
            wcnt += cp_long64(x, buf+wcnt, size-wcnt);
        } else {
            wcnt += cp_byte(TYPE_INT, buf, size-wcnt);
            wcnt += cp_long(x, buf+wcnt, size-wcnt);
        }
    } else if (PyLong_CheckExact(v)) {
        PyLongObject *ob = (PyLongObject *)v;
        wcnt += cp_pylong(ob, buf, size);
    } else if (PyFloat_CheckExact(v)) {
        char *pybuf = PyOS_double_to_string(PyFloat_AS_DOUBLE(v),
                                          'g', 17, 0, NULL);
        BIGFLOW_PYTHON_RAISE_EXCEPTION_IF(!pybuf, "format double to string failed\n");

        int n = strlen(pybuf);
        wcnt += cp_byte(TYPE_FLOAT, buf+wcnt, size-wcnt);
        wcnt += cp_byte((char)n, buf+wcnt, size-wcnt);
        wcnt += cp_string(pybuf, n, buf+wcnt, size-wcnt);
        PyMem_Free(pybuf);
    } else if (PyString_CheckExact(v)) {
        wcnt += cp_byte(TYPE_STRING, buf, size-wcnt);
        int str_len = PyString_GET_SIZE(v);
        wcnt += cp_pstring(PyBytes_AS_STRING(v), str_len, buf + wcnt, size - wcnt);
    } else if (PyTuple_CheckExact(v)) {
        wcnt += cp_byte(TYPE_TUPLE, buf, size-wcnt);
        Py_ssize_t i;
        Py_ssize_t n = PyTuple_Size(v);
        wcnt += cp_long(n, buf+wcnt, size-wcnt);
        for (i = 0; i < n; i++) {
            wcnt += cp_object(PyTuple_GET_ITEM(v, i), buf + wcnt, size - wcnt);
        }
    } else if (PyList_CheckExact(v)) {
        wcnt += cp_byte(TYPE_LIST, buf, size-wcnt);
        Py_ssize_t i;
        Py_ssize_t n;
        n = PyList_GET_SIZE(v);
        wcnt += cp_long(n, buf+wcnt, size-wcnt);
        for (i = 0; i < n; i++) {
            wcnt += cp_object(PyList_GET_ITEM(v, i), buf+wcnt, size-wcnt);
        }
    } else if (PyAnySet_CheckExact(v)) {
        int set_cnt = cp_pyset(v, buf, size);
        BIGFLOW_PYTHON_RAISE_EXCEPTION_IF(set_cnt < 0, "copy pyset failed");
        wcnt += set_cnt;
    } else if (PyUnicode_CheckExact(v)) {
        PyObject *utf8 = PyUnicode_AsUTF8String(v);
        BIGFLOW_PYTHON_RAISE_EXCEPTION_IF(utf8 == NULL, "get utf-8 string failed");
        wcnt += cp_byte(TYPE_UNICODE, buf + wcnt, size - wcnt);
        wcnt += cp_pstring(PyString_AS_STRING(utf8), PyString_GET_SIZE(utf8), buf+wcnt, size-wcnt);
        Py_DECREF(utf8);
    } else if (PyDict_CheckExact(v)) {
        Py_ssize_t pos = 0;
        PyObject *key = NULL;
        PyObject *value = NULL;
        wcnt += cp_byte(TYPE_DICT, buf+wcnt, size-wcnt);
        /* This one is NULL object terminated! */
        pos = 0;
        while (PyDict_Next(v, &pos, &key, &value)) {
            wcnt += cp_object(key, buf+wcnt, size-wcnt);
            wcnt += cp_object(value, buf+wcnt, size-wcnt);
        }
        wcnt += cp_object((PyObject *)NULL, buf+wcnt, size-wcnt);
    } else {
        boost::python::extract<SideInput*> ex(v);
        if (ex.check()) {
            SideInput* si = ex();
            wcnt += cp_object(si->as_list().ptr(), buf+wcnt, size-wcnt);
        } else {
            boost::python::object obj(boost::python::handle<>(boost::python::borrowed(v)));
            BIGFLOW_PYTHON_RAISE_EXCEPTION("meet barshal unsupported type <" + type_string(obj) + ">");
        }
    }
    return wcnt;
}

uint32_t BarshalObjector::Serialize(void* object, char* buffer, uint32_t buffer_size) {
    boost::python::object* obj = static_cast<boost::python::object*>(object);
    int size = cp_object(obj->ptr(), buffer, buffer_size);

    std::string errmsg = "serialize failed, may be this type<"
                         + type_string(*obj) + "> is unsupported for the default serde";

    BIGFLOW_PYTHON_RAISE_EXCEPTION_IF(size < 0, errmsg);
    return (unsigned) size;
}

void* BarshalObjector::Deserialize(const char* buffer, uint32_t buffer_size) {
    try {
        PyObject* serialized = PyMarshal_ReadObjectFromString((char *)buffer, buffer_size);
        boost::python::object *result = new boost::python::object(
             boost::python::handle<>(serialized)
        );
        return result;
    } catch (boost::python::error_already_set) {
        BIGFLOW_HANDLE_PYTHON_EXCEPTION();
    }
}

void BarshalObjector::Release(void* object) {
    delete static_cast<boost::python::object*>(object);
}

} // namespace python
} // namespace bigflow
} // namespace baidu
