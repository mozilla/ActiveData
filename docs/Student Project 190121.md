
# Detecting Change in Uni-Variate Data (GSOC 2019)
(Requires Statistics and Machine Learning)

## Problem

Mozilla tracks hundreds of performance tests. Some measure operations per second, some measure time per operation. All measure a single value, a single variable, over time. Monitoring those tests, and pointing when they change behaviour, is an arduous task. Currently, we use the student's T-test to get a short list of change-points, but it is not good enough: Performance measurements have both seasonal and multi-modal behaviour, which generates too many false positives. Decreasing our alert sensitivity hides the true positives. 

The Gaussian assumption inherent in the T-test is a problem, a better model is needed.

### Definition

Our performance tests are run many times a day, we are looking for points-in-time when they regress: Points before behaved one way, points after behave some other way; either "better" or "worse", but definitely different.  

* **change point** - we use this term to identify the point-in-time when the performance test changed behaviour.

## Solution 

Provide a better model for each performance test: Given a family of statistical models, use machine learning to choose the most likely model. Identify when the performance test deviates from that model.

## Non-solutions

We are analyzing uni-variate data over time, finding points where behaviour likely changed.

* We are not looking for reasons, or features, that caused the perceived change. Although interesting, and maybe even helpful, this project is too big to expand into that space. 
* Most alert systems find exceptional points in the data. This project does not do that; it should be ignoring those one-off exceptions.

## Difficulty 

* Unlike most machine learning applications, this project will not have much data for any one performance test. That's why the plan is to use machine learning to guide statistical model selection.
* The hope is [BIC](https://en.wikipedia.org/wiki/Bayesian_information_criterion), and domain knowledge, will severely restrict model selection; preventing machine learning from approaching a poor fit.   
* This project will be identifying the change-points where behaviour has changed permanently. This is complicated by situations where more than one change-point exists in a given time window.  
* Given a selected model, we should be able to compute likelihood and confidence intervals that data after a change point is selected from that model, or not.

## Steps

All these steps must be considered, by they can be done in any order. It is **expected** that this project will not be completed by one student in one summer.

* **Familiarize oneself with example data** - See example of multimodal test results, see how test perform better on weekends, get a sense for what models will be effectively.
* **Determine the instructions that calculate a better model** - Use familiar tools to build a machine learning algorithm that can find a likely statistical model for any given time series. 
* **Determine instructions that can find change-points** - use familiar tools to identify points in time the data deviate from the given model.  What is the probability the deviation is outside the model?  What is the probablity that the model was wrong?  This requires may assumptions about the family of priors allowed - assumptions are allowed.
* **Prove it is better** - Prove that change-point detection emits less false positives than T-test on our current tests.  

At this point it is assumed we have a prototype that can detect changed performance, but it is in no condition to run in production.

* **Determine the instruction set that can support the above calculations** - We must move off of the familiar tools, and perform the calculation in the Elasticsearch cluster.  What data needs to be pulled?  What basic transformations are required?  What matrix operations are required?  I suspect that these are high level, vector-like operations. Some language specification is expected. 
* **Implement that instruction set** - Implement an interpreter for the language; converting instructions to equivalent Elasticsearch queries; so the majority of the computations are done on the ES cluster.

I am willing to work with the student to match their skills and aptitude to the parts of this project that work best for the student.  

## Existing tech

Mozilla has a 40-node cluster of Elasticsearch machines with multiple terabytes of data. In theory, this is enough compute power to perform the analysis required. It is important that the machine learning code be sent to the data, **we can not move the data to the code**.  T

Elasticsearch has a [machine learning API](https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-apis.html), but it is more about managing machine learning jobs. From demonstrations, it appears to require large data to work properly. 



  
