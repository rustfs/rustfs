# Why rewrite MinIO instead of Ceph?


Both MinIO and Ceph are undoubtedly the most advanced storage products in the world. However, the two companies have taken two different paths. 1.

1. Some companies around the world have directly copied Ceph and released their so-called home-grown, independent and proprietary distributed storage products. 1 million lines of Ceph code, which these companies are unable to maintain, creates a huge data risk for the user. 2;

2. Ceph is too high a technical threshold and too complex to deploy for ordinary SMBs to deploy private storage;

3. Ceph will cause great trouble to the business when rebalancing data;

Based on the above reasons, we chose the simpler and easier-to-use MinIO design to re-implement distributed object storage.