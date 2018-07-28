/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.filter;

import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.filter.predicate.BaseDictionaryBasedPredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class FilterOperatorUtils {

  /**
   * Base penalty associated with predicates for multivalue columns.
   * The cost penalties for single and multivalue columns are chosen to be percentages. The base
   * multivalue penalty is chosen to associate a cost that is *always* higher than single value
   * columns.
   */
  private static final int BASE_MV_PENALTY = 100;

  private FilterOperatorUtils() {
  }

  /**
   * Get the leaf filter operator (i.e. not {@link AndOperator} or {@link OrOperator}).
   */
  public static BaseFilterOperator getLeafFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource,
      int startDocId, int endDocId) {
    if (predicateEvaluator.isAlwaysFalse()) {
      return EmptyFilterOperator.getInstance();
    }

    // Use inverted index if the predicate type is not RANGE or REGEXP_LIKE for efficiency
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    Predicate.Type predicateType = predicateEvaluator.getPredicateType();
    if (dataSourceMetadata.hasInvertedIndex() && (predicateType != Predicate.Type.RANGE) && (predicateType
        != Predicate.Type.REGEXP_LIKE)) {
      if (dataSourceMetadata.isSorted()) {
        return new SortedInvertedIndexBasedFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
      } else {
        return new BitmapBasedFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
      }
    } else {
      return new ScanBasedFilterOperator(predicateEvaluator, dataSource, startDocId, endDocId);
    }
  }

  /**
   * Re-order filter operators based on the their cost. Put the ons with inverted index first so we can process less
   * documents.
   * <p>Special filter operators such as {@link MatchEntireSegmentOperator} and {@link EmptyFilterOperator} should be
   * removed from the list before calling this method.
   */
  public static void reOrderFilterOperators(List<BaseFilterOperator> filterOperators) {
    Collections.sort(filterOperators, new Comparator<BaseFilterOperator>() {
      @Override
      public int compare(BaseFilterOperator o1, BaseFilterOperator o2) {
        return getPriority(o1) - getPriority(o2);
      }

      int getPriority(BaseFilterOperator filterOperator) {
        if (filterOperator instanceof SortedInvertedIndexBasedFilterOperator) {
          return 0;
        }
        if (filterOperator instanceof BitmapBasedFilterOperator) {
          return 1;
        }
        if (filterOperator instanceof AndOperator) {
          return 2;
        }
        if (filterOperator instanceof OrOperator) {
          return 3;
        }
        if (filterOperator instanceof ScanBasedFilterOperator) {
          return getScanBasedFilterPriority(filterOperator, 4);
        }
        throw new IllegalStateException(filterOperator.getClass().getSimpleName()
            + " should not be re-ordered, remove it from the list before calling this method");
      }
    });
  }

  /**
   * Returns the priority for scan based filtering. Multivalue column evaluation is costly, so
   * reorder such that multivalue columns are evaluated after single value columns.
   *
   * Additionally, estimate the cost of the predicate based on percentage of docs that can be scanned.
   * This is proportional to the number of values that *can* be matched by the predicate and the
   * number of docs per value (derived from the column's cardinality).
   *
   * To ensure multivalue columns are assigned a priority that is strictly lower (ie, numerically
   * higher) than the single-value counterparts, we boost the multivalue priority by the max cost
   * for single value columns.
   *
   * @param filterOperator the filter operator to prioritize
   * @return the priority to be associated with the filter
   */
  private static int getScanBasedFilterPriority(BaseFilterOperator filterOperator, int basePriority) {
    DataSourceMetadata metadata = filterOperator.getDataSourceMetadata();
    PredicateEvaluator evaluator = filterOperator.getPredicateEvaluator();
    if (metadata == null || evaluator == null) {
      return basePriority;
    }

    // deprioritize multivalue columns
    if (!metadata.isSingleValue()) {
      basePriority = basePriority + BASE_MV_PENALTY;
    }

    int penalty = 0;
    if (evaluator instanceof BaseDictionaryBasedPredicateEvaluator) {
      int size = metadata.getCardinality();

      Predicate.Type type = evaluator.getPredicateType();
      // obtain an estimate of the number of docs that can be matched
      // for regexp_like, we default to using cardinality
      if (!(type == Predicate.Type.REGEXP_LIKE)) {
        if (type == Predicate.Type.NEQ || type == Predicate.Type.NOT_IN) {
          // use the difference between cardinality and non-matching docs as a proxy for matched count
          size = size - evaluator.getNumNonMatchingDictIds();
        } else {
          size = evaluator.getNumMatchingDictIds();
        }
      }

      int numDocs = metadata.getNumDocs();
      int numDocsPerValue = numDocs/metadata.getCardinality();
      if (numDocsPerValue == 0) {
        numDocsPerValue = 1;
      }
      // get the cost as a percentage of docs scanned - cap it off at 100 (should never be > 100)
      penalty = ((numDocsPerValue * size * 100) / numDocs) % 100;
    }
    return (penalty > 0) ? basePriority + penalty : basePriority;
  }
}
