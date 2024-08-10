#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdlib.h>
#include <math.h>

typedef struct {
    double S;
    double K;
    double T;
    // int dte;
    double r;
    double q;
    char type;
} Option;


double **allocateMatrix(int rows, int cols) {
    double **matrix = (double **)malloc(rows * sizeof(double *));
    for (int i = 0; i < rows; i++) {
        matrix[i] = (double *)malloc(cols * sizeof(double));
    }
    return matrix;
}

void freeMatrix(double **matrix, int rows) {
    for (int i = 0; i < rows; i++) {
        free(matrix[i]);
    }
    free(matrix);
}

double binomialTreePricing(Option opt, double sigma, int N) {
    double deltaT = opt.T / N;
    double u = exp(sigma * sqrt(deltaT));
    double d = 1 / u;
    double p = (exp((opt.r - opt.q) * deltaT) - d) / (u - d);
    double q = 1 - p;
    double discount = exp(-opt.r * deltaT);

    double **priceTree = allocateMatrix(N + 1, N + 1);
    for (int i = 0; i <= N; i++) {
        for (int j = 0; j <= i; j++) {
            priceTree[i][j] = opt.S * pow(u, j) * pow(d, i - j);
        }
    }

    double **valueTree = allocateMatrix(N + 1, N + 1);
    for (int j = 0; j <= N; j++) {
        if (opt.type == 'C') {
            valueTree[N][j] = max(0, priceTree[N][j] - opt.K);
        } else if (opt.type == 'P') {
            valueTree[N][j] = max(0, opt.K - priceTree[N][j]);
        }
    }

    for (int i = N - 1; i >= 0; i--) {
        for (int j = 0; j <= i; j++) {
            double hold = discount * (p * valueTree[i + 1][j + 1] + q * valueTree[i + 1][j]);
            double exercise = (opt.type == 'C') ? max(0, priceTree[i][j] - opt.K) : max(0, opt.K - priceTree[i][j]);
            valueTree[i][j] = max(hold, exercise);
        }
    }

    double optionPrice = valueTree[0][0];

    freeMatrix(priceTree, N + 1);
    freeMatrix(valueTree, N + 1);

    return optionPrice;
}

static PyObject *binomial_tree_pricing(PyObject *self, PyObject *args) {
    Option opt;
    double sigma;
    const char *type;
    int N;

    // if (!PyArg_ParseTuple(args, "ddidddsi", &opt.S, &opt.K, &opt.dte, &opt.r, &sigma, &opt.q, &type, &N)) {
    if (!PyArg_ParseTuple(args, "ddddddsi", &opt.S, &opt.K, &opt.T, &opt.r, &sigma, &opt.q, &type, &N)) {
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

    double price = binomialTreePricing(opt, sigma, N);
    return Py_BuildValue("d", price);
}

static PyMethodDef BinomialTreeMethods[] = {
    {"price", binomial_tree_pricing, METH_VARARGS, "Calculate the price of an American option using the binomial tree method"},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef binomialtreemodule = {
    PyModuleDef_HEAD_INIT,
    "binomialtree",
    NULL,
    -1,
    BinomialTreeMethods
};

PyMODINIT_FUNC PyInit_binomialtree(void) {
    return PyModule_Create(&binomialtreemodule);
}
