/*
    Source: https://towardsdatascience.com/write-your-own-c-extension-to-speed-up-python-x100-626bb9d166e7

*/
/*
    https://docs.python.org/3/faq/windows.html#is-a-pyd-file-the-same-as-a-dll
    You can link to Python in two different ways.
      Load-time linking means linking against
          pythonNN.lib, while run-time linking means
          linking against pythonNN.dll.
          (General note: pythonNN.lib is the so
          -called “import lib” corresponding to
          pythonNN.dll. It merely defines symbols
      for the linker.)
*/
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#define _USE_MATH_DEFINES
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

const double ONE_OVER_SQRT_TWO_PI = 0.3989422804014326779;

typedef struct {
    double S;
    double K;
    double T;
    // int dte;
    double r;
    double q;
    char type;
} Option;


double N(double value)
{
    return 0.5 * erfc(-value * M_SQRT1_2);
}

double d1(double sigma, double S, double K, double T, double r, double sqrt_T)
{
    return (log((double)S / (double)K) + (r + pow(sigma, (double)2) / (double)2) * T) / (double)(sigma * sqrt_T);
}

double d2(double _d1, double sigma, double sqrt_T)
{
    return _d1 - sigma * sqrt_T;
}

double blackScholesPricing(Option opt, double sigma)
{
    double sqrt_T = sqrt(opt.T);
    double _d1 = d1(sigma, opt.S, opt.K, opt.T, opt.r, sqrt_T);
    double _d2 = d2(_d1, sigma, sqrt_T);

    double _nd1;
    double _nd2;
    double price;
    if (opt.type == 'C') {
        _nd1 = N(_d1);
        _nd2 = N(_d2);
        price = opt.S * _nd1 - _nd2 * exp(-opt.r * opt.T) * opt.K;;
    } else {
        _nd1 = N(-_d1);
        _nd2 = N(-_d2);
        price = exp(-opt.r * opt.T) * opt.K * _nd2 - _nd1 * opt.S;
    }
    return price;
}

static PyObject *black_scholes_pricing(PyObject *self, PyObject *args) {
    Option opt;
    double sigma;
    const char *type;

    // if (!PyArg_ParseTuple(args, "ddiddds", &opt.S, &opt.K, &opt.dte, &opt.r, &sigma, &opt.q, &type)) {
    if (!PyArg_ParseTuple(args, "dddddds", &opt.S, &opt.K, &opt.T, &opt.r, &sigma, &opt.q, &type)) {
        return NULL;
    }

    // Set the option type to the first character of the passed string
    opt.type = type[0];

    // double T = (double)(opt.dte / 365.0);

    // // Set the option T
    // opt.T = T;

    // // Check if parsing was successful
    // printf("Parsed arguments: S = %.2f, K = %.2f, dte = %d, T = %.2f, r = %.2f, sigma = %.2f, q = %.2f, N = %d, type = %c\n",
    //        opt.S, opt.K, opt.dte, opt.T, opt.r, sigma, opt.q, N, opt.type);

    double price = blackScholesPricing(opt, sigma);
    return Py_BuildValue("d", price);
}

static PyMethodDef BlackScholesMethods[] = {
    {"price", black_scholes_pricing, METH_VARARGS, "Calculate the price of an European option using the Black-Scholes method"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef blackscholesmodule = {
    PyModuleDef_HEAD_INIT,
    "blacksscholes",
    NULL,
    -1,
    BlackScholesMethods
};

PyMODINIT_FUNC PyInit_blackscholes(void) {
    return PyModule_Create(&blackscholesmodule);
}

