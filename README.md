# Hail

[![Gitter](https://badges.gitter.im/hail-is/hail.svg)](https://gitter.im/hail-is/hail?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge) [![CI Status](https://ci.hail.is/app/rest/builds/buildType:(id:HailSourceCode_HailCi)/statusIcon)](https://ci.hail.is/viewType.html?buildTypeId=HailSourceCode_HailCi&branch_HailSourceCode_HailMainline=%3Cdefault%3E&tab=buildTypeStatusDiv)

[Hail](https://hail.is) is an open-source, scalable framework for exploring and analyzing genomic data. Starting from genetic data in [VCF](https://samtools.github.io/hts-specs/VCFv4.2.pdf), [BGEN](http://www.well.ox.ac.uk/~gav/bgen_format/bgen_format_v1.2.html) or [PLINK](https://www.cog-genomics.org/plink2/formats) format, Hail can, for example:

 - load variant and sample annotations from text tables, JSON, VCF, VEP, and locus interval files
 - generate variant annotations like call rate, Hardy-Weinberg equilibrium p-value, and population-specific allele count
 - generate sample annotations like mean depth, imputed sex, and TiTv ratio
 - generate new annotations from existing ones as well as genotypes, and use these to filter samples, variants, and genotypes
 - find Mendelian violations in trios, prune variants in linkage disequilibrium, analyze genetic similarity between samples via the GRM and IBD matrix, and compute sample scores and variant loadings using PCA
 - perform variant, gene-burden and eQTL association analyses using linear, logistic, and linear mixed regression, and estimate heritability

This functionality and more is exposed through **[Python](https://www.python.org/)** and backed by distributed algorithms built on top of **[Apache Spark](https://spark.apache.org/docs/latest/index.html)** to efficiently analyze gigabyte-scale data on a laptop or terabyte-scale data on a cluster, without the need to manually chop up data or manage job failures. Users can script pipelines or explore data interactively through [Jupyter notebooks](http://jupyter.org/) that flow between Hail with methods for genomics, PySpark with scalable [SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) and [machine learning algorithms](https://spark.apache.org/docs/latest/ml-guide.html), and [pandas](http://pandas.pydata.org/) with [scikit-learn](http://scikit-learn.org/stable/) and [Matplotlib](https://matplotlib.org/) for results that fit on one machine. Hail also provides a flexible domain language to express complex quality control and analysis pipelines with concise, readable code.

The Hail project began in Fall 2015 to empower the worldwide genetics community to [harness the flood of genomes](https://www.broadinstitute.org/blog/harnessing-flood-scaling-data-science-big-genomics-era) to discover the biology of human disease. Hail has been used for dozens of major studies and is the core analysis platform of large-scale genomics efforts such as [gnomAD](http://gnomad.broadinstitute.org/).

Want to get involved in open-source development of methods or infrastructure? Check out the [Github repo](https://github.com/hail-is/hail), chat with us in the [Gitter dev room](https://gitter.im/hail-is/hail-dev), and view our talks at [Spark Summit East](https://spark-summit.org/east-2017/events/scaling-genetic-data-analysis-with-apache-spark/) and [Spark Summit West](https://spark-summit.org/2017/events/scaling-genetic-data-analysis-with-apache-spark/) (below). Or come [join us full-time](jobs.html)!

[![Hail talk at Spark Summit West 2017](https://storage.googleapis.com/hail-common/hail_spark_summit_west.png)](https://www.youtube.com/watch?v=pyeQusIN5Ao&list=PLlMMtlgw6qNjROoMNTBQjAcdx53kV50cS)

#### Getting Started

To get started using Hail on your data or [public data](https://console.cloud.google.com/storage/browser/genomics-public-data/):

- follow the installation instructions in [Getting Started](https://hail.is/hail/getting_started.html)
- check out the [Overview](https://hail.is/hail/overview.html), [Tutorials](https://hail.is/hail/tutorials-landing.html), and [Python API](https://hail.is/hail/api.html)
- chat with the Hail team in the [Hail Gitter](https://gitter.im/hail-is/hail) room

We encourage use of the [Discussion Forum](http://discuss.hail.is) for user and dev support, feature requests, and sharing your Hail-powered science. Follow Hail on Twitter [@hailgenetics](https://twitter.com/hailgenetics). Please report any suspected bugs to [github issues](https://github.com/hail-is/hail/issues).

#### Hail Team

The Hail team is embedded in the [Neale lab](https://nealelab.squarespace.com/) at the [Stanley Center for Psychiatric Research](http://www.broadinstitute.org/scientific-community/science/programs/psychiatric-disease/stanley-center-psychiatric-research/stanle) of the [Broad Institute of MIT and Harvard](http://www.broadinstitute.org) and the [Analytic and Translational Genetics Unit](https://www.atgu.mgh.harvard.edu/) of [Massachusetts General Hospital](http://www.massgeneral.org/).

Contact the Hail team at
<a href="mailto:hail@broadinstitute.org"><code>hail@broadinstitute.org</code></a>.

#### Citing Hail

If you use Hail for published work, please cite the software:

 - Hail, [https://github.com/hail-is/hail](https://github.com/hail-is/hail)

##### Acknowledgements

Hail team uses IntelliJ and <a
href="https://www.yourkit.com/java/profiler/">YourKit Java
Profiler</a>.  We would like to thank
YourKit, LLC for generously supporting open-source
projects with free licenses.  <img
src="https://www.yourkit.com/images/yklogo.png" align="right" />
