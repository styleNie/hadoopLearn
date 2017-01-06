package IGSP;

import java.util.ArrayList;
import java.util.List;

public class Sequence
{
	private int support; // 该序列在数据库中的支持计数
	private ArrayList<Element> sequence;

	public Sequence()
	{
		this.sequence = new ArrayList<Element>();
		this.support = 0;
	}
	public Sequence(Sequence s)
	{
		this.sequence = new ArrayList<Element>();
		this.support = 0;
		for (int i = 0; i < s.size(); i++)
		{
			sequence.add(s.getElement(i));
		}
	}
	public boolean isSubsequenceOf(Sequence s)
	{
		int i = 0, j = 0;
		while (j < s.size() && i < this.sequence.size())
		{
			if (this.getElement(i).toString().equals(s.getElement(j).toString()))
			{
				i++;
				j++;
				if (i == this.sequence.size())
				{
					return true;
				}
			} else
			{
				j++;
			}
		}
		return false;
	}
	public boolean notInSequence(List<Sequence> c)
	{
		for (int i = 0; i < c.size(); i++)
		{
			if (this.toString().equals(c.get(i).toString()))
			{
				return false;
			}
		}
		return true;
	}
	public int size()
	{
		return this.sequence.size();
	}
	public void addElement(Element e)
	{
		sequence.add(e);
	}
	public Element getElement(int index)
	{
		if (index >= 0 && index < sequence.size())
			return sequence.get(index);
		else
			return null;
	}
	public Element removeElement(int index)
	{
		if (index >= 0 && index < sequence.size())
		{
			return sequence.remove(index);
		} else
			return null;
	}
	public Sequence removeFirstElement()
	{
		Sequence sequence = new Sequence();
		for (int i = 1; i < this.size(); i++)
		{
			sequence.addElement(this.getElement(i));
		}
		return sequence;
	}
	public Sequence removeLastElement()
	{
		Sequence sequence = new Sequence();
		for (int i = 0; i < this.size() - 1; i++)
		{
			sequence.addElement(this.getElement(i));
		}
		return sequence;
	}
	@Override
	public String toString()
	{
		return sequence.toString();
	}
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sequence == null) ? 0 : sequence.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Sequence other = (Sequence) obj;
		if (sequence == null)
		{
			if (other.sequence != null)
				return false;
		} else if (!sequence.equals(other.sequence))
			return false;
		return true;
	}
	public int getSupport()
	{
		return support;
	}
	
	public void setSupport(int support)
	{
		this.support = support;
	}
	public void increaseSupport()
	{
		support++;
	}
}
