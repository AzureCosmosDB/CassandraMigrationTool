using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CassandraMigrationProcessor.Context;

namespace CassandraMigrationProcessor.Helpers
{
    public class OrderedUniqueList<T> : IEnumerable<T>
    {
        private readonly List<T> _list = new();
        private readonly HashSet<T> _set = new();

        public void Add(T item)
        {
            MigrationJobContext.AddVerboseLog($"OrderedUniqueList.Add: item={item}, currentCount={_list.Count}");
            if (_set.Add(item)) // returns false if item already exists
                _list.Add(item);
        }

        public bool Remove(T item)
        {
            if (_set.Remove(item))
            {
                _list.Remove(item);
                return true;
            }
            return false;
        }

        public int Count => _list.Count;

        public T this[int index] => _list[index];

        public IEnumerator<T> GetEnumerator() => _list.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Clear()
        {
            _set.Clear();
            _list.Clear();
        }

        public bool Contains(T item) => _set.Contains(item);
    }
}
