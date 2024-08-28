import cvxpy as cp
import numpy as np
import pandas as pd

class PortfolioOptimizer:
    def __init__(self, 
                 data = None,
                 constr_params = None
                 ):
        
        self.data = data
        self.objfunc = None
        self.constr_params = constr_params
        self.constraints = []
        self.n_assets = len(data)
        self.w = cp.Variable(self.n_assets)

    def add_objfunc(self):
        raise NotImplementedError("Subclasses should implement this method.")

    def build_constraints(self):
        raise NotImplementedError("Subclasses should implement this method.")

    def add_constraint_nonneg(self):
         self.constraints += [self.w >= 0]
    
    def add_constraint_notional(self):
         self.constraints += [cp.sum(self.w) == 1]

    def add_constraint_active_exposure(self, group_by = None, metric = None, rel_to = 'Market_Value_Percent', limit = 0.02):
        
        if group_by:
            dummy_matrix = pd.get_dummies(self.data[group_by]).values
            if metric:
                    port_exposure = cp.matmul(dummy_matrix.T @ self.w, self.data[metric].values)
                    bench_exposure = np.matmul(dummy_matrix.T @ self.data[rel_to], self.data[metric].values)
            else:
                    port_exposure = dummy_matrix.T @ self.w
                    bench_exposure = dummy_matrix.T @ self.data[rel_to]
        else:
            if metric:
                    port_exposure = cp.sum(cp.multiply(self.w, self.data[metric].values))
                    bench_exposure = np.dot(self.data[rel_to].values, self.data[metric].values)
            else: 
                    raise ValueError('"metric" has to be provided for portfolio level active constraints')

        self.constraints += [cp.abs(port_exposure - bench_exposure) <= limit]

    def add_custom_constraint(self, custom_constr):
        
        self.constraints += custom_constr

    def optimize(self):
        
        self.add_objfunc()
        self.build_constraints()
        
        prob = cp.Problem(self.objfunc, self.constraints)
        prob.solve()
    
        self.data['Weight_Optimized'] = self.w.value

class MinActiveExposure(PortfolioOptimizer):

    def add_objfunc(self):
        self.objfunc = cp.Minimize(cp.sum_squares(self.w - self.data['Weight'].values))

    def build_constraints(self):
        # nonegativity and notional constraints
        self.add_constraint_nonneg()
        self.add_constraint_notional()

        # active constraints
        for constraint in self.constr_params:
            self.add_constraint_active_exposure(**constraint)

    